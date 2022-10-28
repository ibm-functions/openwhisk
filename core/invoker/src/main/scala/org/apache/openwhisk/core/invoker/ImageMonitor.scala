/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.invoker

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import java.time.Instant
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.database.CouchDbRestClient

import scala.concurrent.{ExecutionContext, Future}
import spray.json.DefaultJsonProtocol._
import spray.json._

case class Action(lru: Long, count: Long)
case class Image(lru: Long, count: Long, actions: Map[String, Action])

/**
 * The image monitor records all invoked docker images and related actions and stores them in the database.
 *
 * The caller is responsible to call `add` on each blackbox invoke and periodically store the images using `write`.
 *
 * @param cluster cluster name.
 * @param invoker invoker instance (number).
 * @param ip invoker ip.
 * @param pod invoker pod name.
 * @param fqname invoker full qualified name (triple).
 * @param staleTime days after which to consider images as stale.
 * @param build functions deploy date (ansible_date_time.iso8601).
 * @param buildNo functions build number (docker tag).
 * @param imageStore images database.
 */
class ImageMonitor(cluster: String,
                   invoker: Int,
                   ip: String,
                   pod: String,
                   fqname: String,
                   staleTime: Int,
                   build: String,
                   buildNo: String,
                   imageStore: CouchDbRestClient)(implicit actorSystem: ActorSystem,
                                                  logging: Logging,
                                                  materializer: ActorMaterializer,
                                                  ec: ExecutionContext) {

  // epoch time after which to consider images as stale
  private val epochDay: Long = 24 * 60 * 60 * 1000
  private val epochStaleTime: Long = epochDay * staleTime

  private val id = s"$cluster/$invoker"
  private var rev = ""

  // docker images used by invoked actions
  private var images: Map[String, Image] = Map.empty
  // ready for monitoring, requires initial sync with image store
  private var ready = false
  // hash code used to check for changes of in-memory image map not yet written to image store
  private var ihash = System.identityHashCode(images)

  def isReady = ready

  /**
   * Transform images in document read from db to map.
   *
   * @param doc images document read from db.
   * @return images map
   */
  private def fromJson(doc: JsObject) = {
    val now = System.currentTimeMillis
    doc
      .fields("images")
      .convertTo[List[JsObject]]
      .map {
        case image =>
          val name = image.fields("name").convertTo[String]
          val lru = image.fields("lru").convertTo[Long]
          val count = image.fields("count").convertTo[Long]
          val actions = image
            .fields("actions")
            .convertTo[List[JsObject]]
            .map {
              case action =>
                val name = action.fields("name").convertTo[String]
                val lru = action.fields("lru").convertTo[Long]
                val count = action.fields("count").convertTo[Long]
                Map(name -> Action(lru, count))
            }
            .flatten
            .toMap
          Map(name -> Image(lru, count, actions))
      }
      .flatten
      .toMap
  }

  /**
   * Write struct log line from images doc that can be queried in logdna
   * (eg println(s"{${'"'}level${'"'}:${'"'}warn${'"'}}"))
   *
   * @param logLevel log level.
   * @param method method.
   * @param msg message.
   * @param doc images document to log.
   */
  private def logJson(logLevel: String, method: String, msg: String, doc: JsObject) = {
    println(
      JsObject(
        "level" -> logLevel.toJson,
        "ts" -> Instant.ofEpochMilli(System.currentTimeMillis).toString.toJson,
        "caller" -> method.toJson,
        "msg" -> msg.toJson,
        "build" -> build.toJson,
        "buildno" -> buildNo.toJson,
        "cluster" -> cluster.toJson,
        "invokerfqname" -> fqname.toJson,
        "invokerinstance" -> invoker.toJson,
        "invokerip" -> ip.toJson,
        "invokerpod" -> pod.toJson,
        "images" -> doc.fields("images")))
  }

  /**
   * Transform images map to JSON and prepare document for database update.
   *
   * @param images map containing images.
   * @param filter filter out stale images and actions.
   * @return JSONfied images
   */
  private def toJson(images: Map[String, Image], filter: Boolean = false) = {
    val now = System.currentTimeMillis
    JsObject(
      "build" -> build.toJson,
      "buildno" -> buildNo.toJson,
      "cluster" -> cluster.toJson,
      "invokerfqname" -> fqname.toJson,
      "invokerinstance" -> invoker.toJson,
      "invokerip" -> ip.toJson,
      "invokerpod" -> pod.toJson,
      "updated" -> now.toJson,
      "updated_at" -> Instant.ofEpochMilli(now).toString.toJson,
      "images" -> (
        if (filter)
          images
            .filter(i => i._2.lru > now - epochStaleTime)
            .toList
            .map {
              case (iname, i) =>
                iname -> Image(i.lru, i.count, i.actions.filter(a => a._2.lru > now - epochStaleTime))
            }
            .toMap
        else images
      ).toList.map {
        case (name, image) =>
          JsObject(
            "name" -> name.toJson,
            "lru" -> image.lru.toJson,
            "count" -> image.count.toJson,
            "actions" -> image.actions.toList.map {
              case (name, action) =>
                JsObject("name" -> name.toJson, "lru" -> action.lru.toJson, "count" -> action.count.toJson)
            }.toJson)
      }.toJson)
  }

  /**
   * Add image and action to map.
   *
   * @param iname image name (eg ibmfunctions/docker-ping).
   * @param aname action name (eg user@de.ibm.com_myspace/EchoMeDockerBlackbox).
   */
  def add(iname: String, aname: String) = this.synchronized {
    val now = System.currentTimeMillis
    images.get(iname) match {
      case None =>
        // new image/action
        images += (iname -> Image(now, 1, Map(aname -> Action(now, 1))))
      case image =>
        image.get.actions.get(aname) match {
          case None =>
            // existing image/new action
            images += (iname -> Image(now, image.get.count + 1, image.get.actions + (aname -> Action(now, 1))))
          case action =>
            // existing image/action
            images += (iname -> Image(
              now,
              image.get.count + 1,
              image.get.actions + (aname -> Action(now, action.get.count + 1))))
        }
    }
  }

  /**
   * Synchronize with image store. Ensure images preload was able to run by checking if invoker config has changed.
   */
  def sync = {
    // initial read doc from image store
    logging.warn(this, s"read $id")
    imageStore
      .getDoc(id)
      .flatMap {
        case Right(doc) =>
          rev = doc.fields("_rev").convertTo[String]
          val docip = doc.fields("invokerip").convertTo[String]
          val docbuild = doc.fields("build").convertTo[String]
          val docbuildno = doc.fields("buildno").convertTo[String]
          images = fromJson(doc)
          ihash = System.identityHashCode(images)
          logging.warn(this, s"read $id($rev), doc: $doc, ihash: $ihash, images: $images")
          logJson("warn", "ImageMonitor", "read images from db", doc)
          // write doc to image store to update metadata (eg invoker ip). after a new deployment
          // zookeeper persistent store is deleted and each invoker will most likely get a new identity (ip)
          imageStore.putDoc(id, rev, toJson(images)).map {
            case Right(res) =>
              rev = res.fields("rev").convertTo[String]
              logging.warn(this, s"written $id($rev)")
              if (ip != docip || build != docbuild) {
                // throw exception if invoker config has changed to enforce a controlled shutdown of the invoker
                // invoker pod will be restarted by kubernetes means and pull runtimes init container is able to
                // preload custom images using the couchdb view showing all images by invoker ip
                throw new Exception(
                  s"invoker config changed for $fqname: $ip($docip), $build($docbuild), $buildNo($docbuildno)")
              }
              ready = true
              ready
            case Left(code) =>
              logging.error(this, s"write $id($rev), error: $code")
              throw new Exception(s"write $id($rev), error: $code")
          }
        case Left(StatusCodes.NotFound) =>
          logging.warn(this, s"read $id, not found")
          val doc = toJson(images)
          logging.warn(this, s"write $id, doc: $doc, ihash: $ihash, images: $images")
          // write new doc to image store
          imageStore.putDoc(id, doc).map {
            case Right(res) =>
              rev = res.fields("rev").convertTo[String]
              logging.warn(this, s"written $id($rev)")
              ready = true
              ready
            case Left(code) =>
              logging.error(this, s"write $id, error: $code")
              throw new Exception(s"write $id, error: $code")
          }
        case Left(code) =>
          logging.error(this, s"read $id, error: $code")
          throw new Exception(s"read $id, error: $code")
      }
  }

  /**
   * Write to image store.
   */
  def write = {
    val imgs = images
    val hash = System.identityHashCode(imgs)
    if (ihash != hash) {
      // image map hash did change, write image map back to store
      val doc = toJson(imgs, true)
      logging.warn(this, s"write $id($rev), doc: $doc, ihash: $hash, images: $imgs($hash)")
      logJson("warn", "ImageMonitor", "write images to db", doc)
      imageStore
        .putDoc(id, rev, doc)
        .flatMap {
          case Right(res) =>
            rev = res.fields("rev").convertTo[String]
            ihash = hash // hash code is not guaranteed to be unique
            logging.warn(this, s"written $id($rev)")
            Future.successful(())
          case Left(StatusCodes.Conflict) =>
            // invalid revision (edge case), try to recover by read again from store to get the valid revision,
            // however image map will not written back before next schedule
            logging.warn(this, s"write $id, conflict, try to recover by read $id")
            imageStore.getDoc(id).map {
              case Right(doc) =>
                rev = doc.fields("_rev").convertTo[String]
                logging.warn(this, s"read $id($rev)")
                ()
              case Left(code) =>
                logging.error(this, s"read $id, error: $code")
                ()
            }
          case Left(code) =>
            logging.error(this, s"write $id($rev), error: $code")
            Future.successful(())
        }
    } else {
      logging.warn(this, s"write $id($rev), no changes")
      Future.successful()
    }.recoverWith {
      case t =>
        logging.error(this, s"read $id($rev), throwable: ${t.getMessage}")
        Future.successful()
    }
  }
}
