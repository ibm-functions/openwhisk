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
 * The namespace blacklist gets all namespaces that are throttled to 0 or blocked from the database.
 *
 * The caller is responsible for periodically updating the blacklist with `refreshBlacklist`.
 *
 * @param imageStore images database with the limit-documents.
 */
class ImageMonitor(cluster: Int = 0, instance: Int, uniqueName: Option[String] = None, imageStore: CouchDbRestClient)(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer,
  ec: ExecutionContext) {

  private val id = s"$cluster/$instance"
  private var rev: Option[String] = None

  private var images: Map[String, Image] = Map.empty

  private def toJson() =
    JsObject(
      "invoker" -> instance.toJson,
      "ip" -> uniqueName.getOrElse(s"10.$instance").toJson,
      "images" -> images.toList.map {
        case (name, image) =>
          JsObject(
            "name" -> name.toJson,
            "lru" -> image.lru.toJson,
            "use" -> image.count.toJson,
            "actions" -> image.actions.toList.map {
              case (name, action) =>
                JsObject("name" -> name.toJson, "lru" -> action.lru.toJson, "use" -> action.count.toJson)
            }.toJson)
      }.toJson)

  def fromJson(response: JsObject) = {
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

  def add(iname: String, aname: String) = {
    val now = System.currentTimeMillis
    val imgs = images.get(iname) match {
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
    images = imgs
  }

  /*if (images.contains(iname)) {
      val image = images.get(iname).get
      if (image.actions.contains(aname)) {
        // existing image/action
        val action = image.actions.get(aname).get
        images + (iname -> Image(new Instant(0), image.count+1, image.actions + (aname -> Action(new Instant(0), action.count+1))))
      } else {
        // existing image but new action
        images + (iname -> Image(new Instant(0), image.count+1, image.actions + (aname -> Action(new Instant(0), 1))))
      }
    } else {
      // new image/action
      images + (iname -> Image(new Instant(0), 1, Map(aname -> Action(new Instant(0), 1))))
    }*/

  def read(): Future[Unit] = {
    imageStore
      .getDoc(id)
      .flatMap {
        case Right(res) =>
          rev = Some(res.fields("rev").convertTo[String])
          images = fromJson(res)
          logging.warn(this, s"read images for $id successful: rev: $rev, res: $res, images: $images")
          Future.successful(())
        case Left(StatusCodes.NotFound) =>
          logging.warn(this, s"read images for $id resuled in notfound")
          Future.successful(())
        case Left(code) =>
          Future.failed(new RuntimeException(s"read images for $id resulted in an error: $code"))
      }
  }

  def write(): Future[Unit] = {
    val json = toJson()
    logging.warn(this, s"write images: $images (json: $json)")
    val request: CouchDbRestClient => Future[Either[StatusCode, JsObject]] = rev match {
      case Some(r) =>
        client =>
          client.putDoc(id, r, json)
      case None =>
        client =>
          client.putDoc(id, json)
    }
    request(imageStore).flatMap {
      case Right(res) =>
        rev = Some(res.fields("rev").convertTo[String])
        logging.warn(this, s"write images for $id successful: rev: $rev")
        Future.successful(())
      case Left(StatusCodes.Conflict) =>
        // A conflict can happen after a crash (write was successful, commit wasn't so it's recomputed). We just
        // do nothing then.
        logging.warn(this, s"write images for $id resulted in a conflict")
        Future.successful(())
      case Left(code) =>
        logging.warn(this, s"write images for $id resulted in an error: $code")
        Future.successful(())
    }
  }
}
