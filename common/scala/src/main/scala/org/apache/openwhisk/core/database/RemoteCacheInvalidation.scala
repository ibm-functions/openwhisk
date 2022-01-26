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

package org.apache.openwhisk.core.database

import java.nio.charset.StandardCharsets

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import akka.actor.ActorSystem
import akka.actor.Props
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.connector.Message
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.CacheKey
import org.apache.openwhisk.core.entity.ControllerInstanceId
import org.apache.openwhisk.core.entity.WhiskAction
import org.apache.openwhisk.core.entity.WhiskActionMetaData
import org.apache.openwhisk.core.entity.WhiskEntity
import org.apache.openwhisk.core.entity.WhiskPackage
import org.apache.openwhisk.core.entity.WhiskRule
import org.apache.openwhisk.core.entity.WhiskTrigger
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._

case class CacheInvalidationMessage(key: CacheKey, instanceId: String) extends Message {
  override def serialize = CacheInvalidationMessage.serdes.write(this).compactPrint
}

object CacheInvalidationMessage extends DefaultJsonProtocol {
  def parse(msg: String) = Try(serdes.read(msg.parseJson))
  implicit val serdes = jsonFormat(CacheInvalidationMessage.apply _, "key", "instanceId")
}

class RemoteCacheInvalidation(config: WhiskConfig, component: String, instance: ControllerInstanceId)(
  implicit logging: Logging,
  as: ActorSystem) {
  import RemoteCacheInvalidation._
  implicit private val ec = as.dispatchers.lookup("dispatchers.kafka-dispatcher")

  private val instanceId = s"$component${instance.asString}"

  private val msgProvider = SpiLoader.get[MessagingProvider]
  private val cacheInvalidationConsumer =
    msgProvider.getConsumer(config, s"$cacheInvalidationTopic$instanceId", cacheInvalidationTopic, maxPeek = 128)
  private val cacheInvalidationProducer = msgProvider.getProducer(config)

  private val dbConfig = loadConfigOrThrow[CouchDbConfig](ConfigKeys.couchdb)
  private val dbClient: CouchDbRestClient =
    new CouchDbRestClient(
      dbConfig.protocol,
      dbConfig.host,
      dbConfig.port,
      dbConfig.username,
      dbConfig.password,
      dbConfig.databaseFor[WhiskEntity])

  private val lcuskey = "last_seq" // last change update sequence key
  private var lcus = "" // last change update sequence
  private var lcusnum = -1 // num last change update sequences

  def ensureLastChangeUpdateSequence() = {

    dbClient
      .changes()(limit = Some(1), descending = true)
      .map {
        case Right(response) =>
          lcus = Try(response.fields(lcuskey).asInstanceOf[JsString].convertTo[String]).getOrElse("")
          assert(!lcus.isEmpty, s"no or invalid last change update sequence in response: '$response'")
          logging.info(this, s"@StR initial last change update sequence: $lcus")

          Scheduler.scheduleWaitAtLeast(interval = 15.seconds, initialDelay = 60.seconds, name = "CacheInvalidation")(
            () => {
              val limit = 10
              do {
                lcusnum = -1 // reset num of fetched seqs
                dbClient
                  .changes()(since = Some(lcus), limit = Some(limit), descending = false)
                  .map {
                    case Right(response) =>
                      val newlcus = response.fields(lcuskey).asInstanceOf[JsString].convertTo[String]
                      logging.info(this, s"@StR lcus: $lcus, newlcus: $newlcus")
                      val seqs = response.fields("results").convertTo[List[JsObject]]
                      val seqsdel = seqs.filter(_.fields.contains("deleted"))
                      logging.info(this, s"@StR found ${seqs.length} changes (${seqsdel.length} deletions)")
                      if (seqs.length > 0) {
                        logging.info(
                          this,
                          s"@StR cache before invalidation: " +
                            s"actmetasize: ${WhiskActionMetaData.cacheSize}, " +
                            s"actsize: ${WhiskAction.cacheSize}, " +
                            s"pkgsize: ${WhiskPackage.cacheSize}, " +
                            s"rulesize: ${WhiskRule.cacheSize}, " +
                            s"trgsize: ${WhiskTrigger.cacheSize}")

                        seqs.foreach {
                          seq =>
                            // [RemoteCacheInvalidation] @StR msg: {"instanceId":"controller1001","key":{"mainId":"srost@de.ibm.com_myspace/strxxx"}},
                            val ck = CacheKey(seq.fields("id").asInstanceOf[JsString].convertTo[String])
                            logging.info(this, s"@StR going to remove key from cache: $ck")
                            WhiskActionMetaData.removeId(ck)
                            WhiskAction.removeId(ck)
                            WhiskPackage.removeId(ck)
                            WhiskRule.removeId(ck)
                            WhiskTrigger.removeId(ck)
                        }

                        logging.info(
                          this,
                          s"@StR cache after invalidation: " +
                            s"actmetasize: ${WhiskActionMetaData.cacheSize}, " +
                            s"actsize: ${WhiskAction.cacheSize}, " +
                            s"pkgsize: ${WhiskPackage.cacheSize}, " +
                            s"rulesize: ${WhiskRule.cacheSize}, " +
                            s"trgsize: ${WhiskTrigger.cacheSize}")
                      }

                      lcus = newlcus
                      lcusnum = seqs.length
                      logging.info(this, s"@StR new last change update sequence: $lcus")

                    case Left(code) =>
                      logging.error(this, s"Unexpected http response code: $code, keep old lcus '$lcus'")
                  }
                  .recoverWith {
                    case t =>
                      logging.error(
                        this,
                        s"@StR '${dbConfig.databaseFor[WhiskEntity]}' internal error for _changes call, failure: '${t.getMessage}', keep old lcus '$lcus'")
                      Future(Success(lcus))
                  }
                if (lcusnum == limit) {
                  logging.info(this, s"@StR pagination..")
                }
              } while (lcusnum == limit) // continue in case of pending changes
              Future(Success(lcus))
            })
          Success(lcus)

        case Left(code) =>
          val msg = s"@StR '${dbConfig.databaseFor[WhiskEntity]}' unexecpted response code: $code from _changes call"
          logging.error(this, msg)
          Failure(new Throwable(msg))
      }
      .recoverWith {
        case t =>
          val msg = s"@StR '${dbConfig.databaseFor[WhiskEntity]}' internal error, failure: '${t.getMessage}'"
          logging.error(this, msg)
          Future(Failure(new Throwable(msg)))
      }
  }

  def notifyOtherInstancesAboutInvalidation(key: CacheKey): Future[Unit] = {
    cacheInvalidationProducer.send(cacheInvalidationTopic, CacheInvalidationMessage(key, instanceId)).map(_ => Unit)
  }

  private val invalidationFeed = as.actorOf(Props {
    new MessageFeed(
      "cacheInvalidation",
      logging,
      cacheInvalidationConsumer,
      cacheInvalidationConsumer.maxPeek,
      1.second,
      removeFromLocalCache)
  })

  def invalidateWhiskActionMetaData(key: CacheKey) =
    WhiskActionMetaData.removeId(key)

  private def removeFromLocalCache(bytes: Array[Byte]): Future[Unit] = Future {
    val raw = new String(bytes, StandardCharsets.UTF_8)

    CacheInvalidationMessage.parse(raw) match {
      case Success(msg: CacheInvalidationMessage) => {
        logging.warn(this, s"@StR skip cache invalidation via kafka message")
        /*logging.warn(
          this,
          s"@StR msg: $msg, " +
            s"actmetasize: ${WhiskActionMetaData.cacheSize}, " +
            s"actsize: ${WhiskAction.cacheSize}, " +
            s"pkgsize: ${WhiskPackage.cacheSize}, " +
            s"rulesize: ${WhiskRule.cacheSize}, " +
            s"trgsize: ${WhiskTrigger.cacheSize}")
        if (msg.instanceId != instanceId) {
          WhiskActionMetaData.removeId(msg.key)
          WhiskAction.removeId(msg.key)
          WhiskPackage.removeId(msg.key)
          WhiskRule.removeId(msg.key)
          WhiskTrigger.removeId(msg.key)
          logging.warn(
            this,
            s"@StR " +
              s"actmetasize: ${WhiskActionMetaData.cacheSize}, " +
              s"actsize: ${WhiskAction.cacheSize}, " +
              s"pkgsize: ${WhiskPackage.cacheSize}, " +
              s"rulesize: ${WhiskRule.cacheSize}, " +
              s"trgsize: ${WhiskTrigger.cacheSize}")
        }*/
      }
      case Failure(t) => logging.error(this, s"failed processing message: $raw with $t")
    }
    invalidationFeed ! MessageFeed.Processed
  }
}

object RemoteCacheInvalidation {
  val cacheInvalidationTopic = "cacheInvalidation"
}
