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
 * @param uniqueName unique name (ip).
 * @param imageStore images database.
 */
class ImageMonitor(cluster: Int,
                   invoker: Int,
                   uniqueName: Option[String] = None,
                   staleTime: Int,
                   imageStore: CouchDbRestClient)(implicit actorSystem: ActorSystem,
                                                  logging: Logging,
                                                  materializer: ActorMaterializer,
                                                  ec: ExecutionContext) {

  private val id = s"$cluster/$invoker"
  private var rev = ""

  private var images: Map[String, Image] = Map.empty
  // one-time synced (initialized at the beginning) with image store
  private var initialized = false
  // image hash code used to check for pending changes not yet written back to image store
  private var ihash = System.identityHashCode(images)

  private def toJson() =
    JsObject(
      "invoker" -> invoker.toJson,
      "ip" -> uniqueName.getOrElse(s"$invoker").toJson,
      "images" -> images.toList.map {
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

  private def fromJson(response: JsObject) = {
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
  }

  def add(iname: String, aname: String) = this.synchronized {
    if (initialized) {
      val now = System.currentTimeMillis
      images = images.get(iname) match {
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
    }
  }

  def sync: Future[Unit] = {
    if (!initialized) {
      read
    } else {
      write
    }
  }

  private def read: Future[Unit] = {
    logging.warn(this, s"read for $id")
    imageStore
      .getDoc(id)
      .flatMap {
        case Right(res) =>
          rev = res.fields("rev").convertTo[String]
          images = fromJson(res)
          ihash = System.identityHashCode(images)
          logging.warn(this, s"read for $id($rev), json: $res, images: $images($ihash)")
          initialized = true
          Future.successful(())
        case Left(StatusCodes.NotFound) =>
          logging.warn(this, s"read for $id: not found")
          val json = toJson()
          logging.warn(this, s"write for $id, images: $images($ihash), json: $json")
          imageStore.putDoc(id, json).flatMap {
            case Right(res) =>
              rev = res.fields("rev").convertTo[String]
              logging.warn(this, s"write for $id($rev)")
              initialized = true
              Future.successful(())
            case Left(code) =>
              logging.error(this, s"write for $id: error: $code")
              Future.successful(())
          }
        case Left(code) =>
          logging.error(this, s"read for $id: error: $code")
          Future.successful(())
      }
  }

  private def write(): Future[Unit] = {
    val hash = System.identityHashCode(images)
    if (ihash != hash) {
      val json = toJson()
      logging.warn(this, s"write for $id($rev): images: $images($hash), json: $json")
      imageStore.putDoc(id, json).flatMap {
        case Right(res) =>
          rev = res.fields("rev").convertTo[String]
          ihash = hash // hash code is not guaranteed to be unique
          logging.warn(this, s"write for $id($rev)")
          Future.successful(())
        case Left(code) =>
          logging.error(this, s"write for $id($rev), error: $code")
          Future.successful(())
      }
    } else {
      logging.warn(this, s"write for $id($rev), no pending changes")
      Future.successful(())
    }
  }
}
