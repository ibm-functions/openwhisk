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
                                           pageSize: Int,
                                           maxPages: Int)
  private val cacheInvalidationConfigNamespace = "whisk.controller.cacheinvalidation"
  private val cacheInvalidationConfig = loadConfig[CacheInvalidationConfig](cacheInvalidationConfigNamespace).toOption
  private val cacheInvalidationEnabled = cacheInvalidationConfig.exists(_.enabled)
  private val cacheInvalidationInitDelay = cacheInvalidationConfig.map(_.initDelay).getOrElse(-1)
  private val cacheInvalidationPollInterval = cacheInvalidationConfig.map(_.pollInterval).getOrElse(-1)
  private val cacheInvalidationPageSize = cacheInvalidationConfig.map(_.pageSize).getOrElse(-1)
  private val cacheInvalidationMaxPages = cacheInvalidationConfig.map(_.maxPages).getOrElse(-1)
  logging.info(
    this,
    s"cacheInvalidationEnabled: $cacheInvalidationEnabled, " +
      s"cacheInvalidationInitDelay: $cacheInvalidationInitDelay, " +
      s"cacheInvalidationPollInterval: $cacheInvalidationPollInterval, " +
      s"cacheInvalidationPageSize: $cacheInvalidationPageSize, " +
      s"cacheInvalidationMaxPages: $cacheInvalidationMaxPages")

  private val dbConfig = loadConfigOrThrow[CouchDbConfig](ConfigKeys.couchdb)
  private val dbClient: CouchDbRestClient =
    new CouchDbRestClient(
      dbConfig.protocol,
      dbConfig.host,
      dbConfig.port,
      dbConfig.username,
      dbConfig.password,
      dbConfig.databaseFor[WhiskEntity])

  class LastChangeUpdateSequence()(implicit logging: Logging) {

    private var lastChangeUpdateSequence: Option[String] = None

    /**
     * Get last change update sequence from store.
     *
     * @return last change update sequence
     */
    def get(): String = {
      assert(lastChangeUpdateSequence.isDefined, s"invalid lcus: '$lastChangeUpdateSequnce'")
      lastChangeUpdateSequence.get
    }

    /**
     * Get last change update sequence from db response.
     *
     * @return last change update sequence
     */
    def get(resp: JsObject): String = {
      val lcus = resp.fields("last_seq").asInstanceOf[JsString].convertTo[String]
      assert(!lcus.isEmpty, s"invalid lcus: '$lcus'")
      lcus
    }

    /**
     * Set last change update sequence from db response to store.
     */
    def set(resp: JsObject): Unit = {
      val lcus = get(resp)
      lastChangeUpdateSequence match {
        case None =>
          logging.info(this, s"initial lcus: $lcus")
        case _ =>
          logging.info(this, s"new lcus: $lcus (old lcus: ${lastChangeUpdateSequence.get}")
      }
      lastChangeUpdateSequence = Some(lcus)
    }

    /**
     * Get last change update sequences from db response.
     *
     * @return list last change update sequences
     */
    def getSeqs(resp: JsObject): List[JsObject] = {
      resp.fields("results").convertTo[List[JsObject]]
    }

    /**
     * Get last change update sequences for deletions count from db sequences.
     *
     * @return last change update sequences for deletions count
     */
    def getCountDelSeqs(seqs: List[JsObject]): Int = {
      seqs.count(_.fields.contains("deleted"))
    }

    /**
     * Get last change update sequence doc id from db sequence.
     *
     * @return last change update sequence doc id
     */
    def getSeqDocId(seq: JsObject): String = {
      seq.fields("id").asInstanceOf[JsString].convertTo[String]
    }
  }

  private val lastChangeUpdateSequnce = new LastChangeUpdateSequence

  private def removeFromLocalCacheBySeqs(resp: JsObject): Int = {
    val seqs = lastChangeUpdateSequnce.getSeqs(resp)
    logging.info(this, s"found ${seqs.length} changes (${lastChangeUpdateSequnce.getCountDelSeqs(seqs)} deletions)")
    seqs.foreach { seq =>
      val ck = CacheKey(lastChangeUpdateSequnce.getSeqDocId(seq))
      WhiskActionMetaData.removeId(ck)
      WhiskAction.removeId(ck)
      WhiskPackage.removeId(ck)
      WhiskRule.removeId(ck)
      WhiskTrigger.removeId(ck)
      logging.debug(this, s"removed key $ck from cache")
    }
    seqs.length
  }

  //@tailrec
  private def getLastChangeUpdateSequences(lcus: String, limit: Int, pages: Int): Future[Unit] = {
    require(limit >= 0, "limit should be non negative")
    dbClient
      .changes()(since = Some(lcus), limit = Some(limit), descending = false)
      .map {
        case Right(resp) =>
          removeFromLocalCacheBySeqs(resp) match {
            case ps if ps == limit && pages > 0 =>
              logging.info(this, s"fetched max changes ($limit) ($pages pages left)")
              getLastChangeUpdateSequences(lastChangeUpdateSequnce.get(resp), limit, pages - 1)
            case ps if ps > 0 || pages == 0 =>
              lastChangeUpdateSequnce.set(resp)
            case _ =>
          }
        case Left(code) =>
          logging.error(this, s"unexpected response code: $code")
      }
      .recoverWith {
        case t: Throwable =>
          logging.error(this, s"'${dbConfig.databaseFor[WhiskEntity]}' internal error: '${t.getMessage}'")
          Future.successful(())
      }
      .mapTo[Unit]
  }

  def scheduleCacheInvalidation(): Any = {
    if (cacheInvalidationEnabled) {
      Scheduler.scheduleWaitAtLeast(
        interval = FiniteDuration(cacheInvalidationPollInterval, TimeUnit.SECONDS),
        initialDelay = FiniteDuration(cacheInvalidationInitDelay, TimeUnit.SECONDS),
        name = "CacheInvalidation") { () =>
        getLastChangeUpdateSequences(
          lastChangeUpdateSequnce.get(),
          cacheInvalidationPageSize,
          cacheInvalidationMaxPages - 1)
      }
    }
  }

  def ensureInitialLastChangeUpdateSequence(): Future[Unit] = {
    if (cacheInvalidationEnabled) {
      dbClient
        .changes()(limit = Some(1), descending = true)
        .map {
          case Right(resp) =>
            lastChangeUpdateSequnce.set(resp)
          case Left(code) =>
            throw new Throwable(s"unexpected response code: $code")
        }
        .recoverWith {
          case t: Throwable =>
            throw new Throwable(s"'${dbConfig.databaseFor[WhiskEntity]}' internal error: '${t.getMessage}'")
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

  def invalidateWhiskActionMetaData(key: CacheKey): Unit =
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
