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
import java.util.concurrent.TimeUnit

//import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
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

  // config for controller cache invalidation
  final case class CacheInvalidationConfig(enabled: Boolean,
                                           initDelay: Int,
                                           pollInterval: Int,
                                           pollLimit: Int,
                                           recursionDepth: Int)
  private val cacheInvalidationConfigNamespace = "whisk.controller.cacheinvalidation"
  private val cacheInvalidationConfig = loadConfig[CacheInvalidationConfig](cacheInvalidationConfigNamespace).toOption
  private val cacheInvalidationEnabled = cacheInvalidationConfig.map(_.enabled).getOrElse(false)
  private val cacheInvalidationInitDelay = cacheInvalidationConfig.map(_.initDelay).getOrElse(-1)
  private val cacheInvalidationPollInterval = cacheInvalidationConfig.map(_.pollInterval).getOrElse(-1)
  private val cacheInvalidationPollLimit = cacheInvalidationConfig.map(_.pollLimit).getOrElse(-1)
  private val cacheInvalidationRecursionDepth = cacheInvalidationConfig.map(_.recursionDepth).getOrElse(10)
  logging.info(
    this,
    s"cacheInvalidationEnabled: ${cacheInvalidationEnabled}, " +
      s"cacheInvalidationInitDelay: ${cacheInvalidationInitDelay}, " +
      s"cacheInvalidationPollInterval: ${cacheInvalidationPollInterval}, " +
      s"cacheInvalidationPollLimit: ${cacheInvalidationPollLimit}, " +
      s"cacheInvalidationRecursionDepth: ${cacheInvalidationRecursionDepth}")

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

  private def removeFromLocalCacheBySeqs(seqs: List[JsObject]) = {
    if (seqs.length > 0) {
      seqs.foreach { seq =>
        val ck = CacheKey(seq.fields("id").asInstanceOf[JsString].convertTo[String])
        WhiskActionMetaData.removeId(ck)
        WhiskAction.removeId(ck)
        WhiskPackage.removeId(ck)
        WhiskRule.removeId(ck)
        WhiskTrigger.removeId(ck)
        logging.debug(this, s"removed key $ck from cache")
      }
    }
  }

  //@tailrec
  private def getLastChangeUpdateSequences(limit: Int, recursions: Int): Future[Unit] = {
    require(limit >= 0, "limit should be non negative")
    dbClient
      .changes()(since = Some(lcus), limit = Some(limit), descending = false)
      .map {
        case Right(resp) =>
          val newlcus = resp.fields(lcuskey).asInstanceOf[JsString].convertTo[String]
          val seqs = resp.fields("results").convertTo[List[JsObject]]
          logging.info(
            this,
            s"found ${seqs.length} changes (${seqs.filter(_.fields.contains("deleted")).length} deletions)")
          removeFromLocalCacheBySeqs(seqs)

          lcus = newlcus
          logging.info(this, s"new last change update sequence: $lcus")

          if (seqs.length == limit && recursions >= 1) {
            logging.warn(this, s"fetched maximum limit of $limit($recursions) changes from db")
            getLastChangeUpdateSequences(limit, recursions - 1)
          }

        case Left(code) =>
          logging.error(this, s"unexpected response code $code")
          Future.successful(())
      }
      .recoverWith {
        case t: Throwable =>
          logging.error(this, s"'${dbConfig.databaseFor[WhiskEntity]}' internal error: '${t.getMessage}'")
          Future.successful(())
      }
      .mapTo[Unit]
  }

  def scheduleCacheInvalidation() = {
    if (cacheInvalidationEnabled) {
      Scheduler.scheduleWaitAtLeast(
        interval = FiniteDuration(cacheInvalidationPollInterval, TimeUnit.SECONDS),
        initialDelay = FiniteDuration(cacheInvalidationInitDelay, TimeUnit.SECONDS),
        name = "CacheInvalidation") { () =>
        getLastChangeUpdateSequences(cacheInvalidationPollLimit, cacheInvalidationRecursionDepth)
      }
    }
  }

  def ensureInitialLastChangeUpdateSequence() = {
    if (cacheInvalidationEnabled) {
      dbClient
        .changes()(limit = Some(1), descending = true)
        .map {
          case Right(response) =>
            lcus = response.fields(lcuskey).asInstanceOf[JsString].convertTo[String]
            assert(!lcus.isEmpty, s"invalid initial last change update sequence in response: '$response'")
            logging.info(this, s"initial last change update sequence: $lcus")
          case Left(code) =>
            val msg = s"unexecpted response code: $code from _changes call"
            logging.error(this, msg)
            throw new Throwable(msg)
        }
        .recoverWith {
          case t =>
            val msg = s"'${dbConfig.databaseFor[WhiskEntity]}' internal error: '${t.getMessage}'"
            logging.error(this, msg)
            throw t
        }
    } else {
      Future.successful(())
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
        if (msg.instanceId != instanceId) {
          WhiskActionMetaData.removeId(msg.key)
          WhiskAction.removeId(msg.key)
          WhiskPackage.removeId(msg.key)
          WhiskRule.removeId(msg.key)
          WhiskTrigger.removeId(msg.key)
        }
      }
      case Failure(t) => logging.error(this, s"failed processing message: $raw with $t")
    }
    invalidationFeed ! MessageFeed.Processed
  }
}

object RemoteCacheInvalidation {
  val cacheInvalidationTopic = "cacheInvalidation"
}
