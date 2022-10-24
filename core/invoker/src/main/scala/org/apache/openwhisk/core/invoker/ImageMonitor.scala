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
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.ActorMaterializer
import java.time.Instant
import org.apache.openwhisk.common.{Logging}
import org.apache.openwhisk.core.database.{CouchDbRestClient}

import scala.concurrent.{ExecutionContext, Future}
import spray.json.DefaultJsonProtocol._
import spray.json._

case class Action(lru: Long, count: Long)
case class Image(lru: Long, count: Long, actions: Map[String, Action] = Map.empty)

/**
 * The image monitor records all invokedblackbox images and related actions and stores them in the database.
 *
 * The caller is responsible to call `add` on each blackbox invoke and periodically store the images using `sync`.
 *
 * @param cluster cluster instance.
 * @param invoker invoker instance
 * @param ip ip (unique name).
 * @param pod invoker pod name.
 * @param fqname full qualified name.
 * @param staleTime days after which to consider images as stale.
 * @param buildNo functions build notafter which to consider images as stale.
 * @param imageStore images database.
 */
class ImageMonitor(cluster: Int,
                   invoker: Int,
                   ip: Option[String],
                   pod: Option[String],
                   fqname: String,
                   staleTime: Int,
                   buildNo: String,
                   deployDate: String,
                   imageStore: CouchDbRestClient)(implicit actorSystem: ActorSystem,
                                                  logging: Logging,
                                                  materializer: ActorMaterializer,
                                                  ec: ExecutionContext) {

  // epoch time after which to consider images as stale
  private val epochDay: Long = 24 * 60 * 60 * 1000
  private val epochStaleTime: Long = epochDay * staleTime

  private val id = s"$cluster/$invoker"
  private var rev = ""

  private var images: Map[String, Image] = Map.empty
  // initially synced at the beginning with image store
  private var initsync = false
  // image hash code used to check for pending changes not yet written back to image store
  private var ihash = System.identityHashCode(images)

  private def toJsonLog(logLevel: String, method: String, msg: String, doc: JsObject) = {
    val now = System.currentTimeMillis
    JsObject(
      "level" -> logLevel.toJson,
      "ts" -> Instant.ofEpochMilli(now).toString.toJson,
      "caller" -> method.toJson,
      "msg" -> msg.toJson,
      "invoker" -> invoker.toJson,
      "ip" -> ip.getOrElse("").toJson,
      "pod" -> pod.getOrElse("").toJson,
      "fqname" -> fqname.toJson,
      "buildno" -> buildNo.toJson,
      "build" -> deployDate.toJson,
      "images" -> doc.fields("images"))
  }

  private def toJson(images: Map[String, Image]) = {
    val now = System.currentTimeMillis
    JsObject(
      "invoker" -> invoker.toJson,
      "ip" -> ip.getOrElse("").toJson,
      "pod" -> pod.getOrElse("").toJson,
      "fqname" -> fqname.toJson,
      "buildno" -> buildNo.toJson,
      "build" -> deployDate.toJson,
      "updated" -> now.toJson,
      "images" -> images
        .filter(i => i._2.lru > now - epochStaleTime) // filter out stale images
        .toList
        .map {
          case (name, image) =>
            JsObject(
              "name" -> name.toJson,
              "lru" -> image.lru.toJson,
              "count" -> image.count.toJson,
              "actions" -> image.actions.toList.map {
                case (name, action) =>
                  JsObject("name" -> name.toJson, "lru" -> action.lru.toJson, "count" -> action.count.toJson)
              }.toJson)
        }
        .toJson)
  }

  private def fromJson(response: JsObject) = {
    val now = System.currentTimeMillis
    response
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
          Map(name -> Image(lru, count))
      }
      .flatten
      .toMap
      .filter(i => i._2.lru > now - epochStaleTime) // filter out stale images
  }

  def add(iname: String, aname: String) = this.synchronized {
    if (initsync && iname.length > 0) {
      val now = System.currentTimeMillis
      val newImages = images.get(iname) match {
        case None =>
          // new image/action
          images + (iname -> Image(now, 1, Map(aname -> Action(now, 1))))
        case image =>
          image.get.actions.get(aname) match {
            case None =>
              // existing image/new action
              images + (iname -> Image(now, image.get.count + 1, image.get.actions + (aname -> Action(now, 1))))
            case action =>
              // existing image/action
              images + (iname -> Image(
                now,
                image.get.count + 1,
                image.get.actions + (aname -> Action(now, action.get.count + 1))))
          }
      }
      images = newImages
    }
    images
  }

  def sync = {
    if (initsync) { write } else { read }
  }

  private def read = {
    logging.warn(this, s"read $id")
    imageStore
      .getDoc(id)
      .flatMap {
        case Right(doc) =>
          rev = doc.fields("_rev").convertTo[String]
          images = fromJson(doc)
          ihash = System.identityHashCode(images)
          logging.warn(this, s"read $id($rev), doc: $doc, images: $images($ihash)")
          // write structured log line that can be queried in logdna
          println(toJsonLog("warn", "ImageMonitor", "read images from db", doc))
          //println(s"{${'"'}level${'"'}:${'"'}warn${'"'}}")
          initsync = true
          Future.successful(())
        case Left(StatusCodes.NotFound) =>
          logging.warn(this, s"read $id, not found")
          val doc = toJson(add("", ""))
          logging.warn(this, s"write $id, doc: $doc, images: $images($ihash)")
          imageStore.putDoc(id, doc).flatMap {
            case Right(res) =>
              rev = res.fields("rev").convertTo[String]
              logging.warn(this, s"written $id($rev)")
              initsync = true
              Future.successful(())
            case Left(code) =>
              logging.error(this, s"write $id, error: $code")
              Future.successful(())
          }
        case Left(code) =>
          logging.error(this, s"read $id, error: $code")
          Future.successful(())
      }
      .recoverWith {
        case t =>
          logging.error(this, s"read $id, throwable: ${t.getMessage}")
          Future.successful(())
      }
  }

  private def write = {
    val hash = System.identityHashCode(images)
    if (ihash != hash) {
      val images = add("", "")
      val doc = toJson(images)
      logging.warn(this, s"write $id($rev), doc: $doc, images: $images($hash)")
      // write struct log line that can be queried in logdna
      println(toJsonLog("warn", "ImageMonitor", "write images to db", doc))
      imageStore
        .putDoc(id, rev, doc)
        .flatMap {
          case Right(res) =>
            rev = res.fields("rev").convertTo[String]
            ihash = hash // hash code is not guaranteed to be unique
            logging.warn(this, s"written $id($rev)")
            Future.successful(())
          case Left(StatusCodes.Conflict) =>
            // edge case (invalid revision), try to recover by read again from db
            logging.warn(this, s"write $id, conflict, try to recover by read $id")
            imageStore.getDoc(id).flatMap {
              case Right(doc) =>
                rev = doc.fields("_rev").convertTo[String]
                logging.warn(this, s"read $id($rev)")
                Future.successful(())
              case Left(code) =>
                logging.error(this, s"read $id, error: $code")
                Future.successful(())
            }
          case Left(code) =>
            logging.error(this, s"write $id($rev), error: $code")
            Future.successful(())
        }
        .recoverWith {
          case t =>
            logging.error(this, s"write $id($rev), throwable: ${t.getMessage}")
            Future.successful(())
        }
    } else {
      logging.warn(this, s"write $id($rev), no changes")
      Future.successful(())
    }
  }
}
