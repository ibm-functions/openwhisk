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

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
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

  class DbChanges()(implicit logging: Logging) {

    private val dbConfig = loadConfigOrThrow[CouchDbConfig](ConfigKeys.couchdb)
    private val dbClient: CouchDbRestClient =
      new CouchDbRestClient(
        dbConfig.protocol,
        dbConfig.host,
        dbConfig.port,
        dbConfig.username,
        dbConfig.password,
        dbConfig.databaseFor[WhiskEntity])

    private var lcus: String = ""

    /**
     * Store initial last change update sequence.
     */
    def ensureInitialSequence(): Future[Unit] = {
      assert(lcus.isEmpty, s"lcus: 'initial $lcus' is already set")
      dbClient
        .changes()(limit = Some(1), descending = true)
        .map { resp =>
          lcus = resp.fields("last_seq").asInstanceOf[JsString].convertTo[String]
          logging.info(this, s"initial lcus: $lcus")
        }
    }

    /**
     * Get changes made to documents in the database and store last change update sequence.
     *
     * @return ids of changed documents
     */
    def getChanges(limit: Int): Future[List[String]] = {
      require(limit >= 0, "limit should be non negative")
      dbClient
        .changes()(since = Some(lcus), limit = Some(limit), descending = false)
        .map { resp =>
          val seqs = resp.fields("results").convertTo[List[JsObject]]
          logging.info(this, s"found ${seqs.length} changes (${seqs.count(_.fields.contains("deleted"))} deletions)")
          if (seqs.length > 0) {
            lcus = resp.fields("last_seq").asInstanceOf[JsString].convertTo[String]
            logging.info(this, s"new lcus: $lcus")
          }
          seqs.map(_.fields("id").convertTo[String])
        }
    }
  }

  private val dbChanges = new DbChanges()

  private def removeFromLocalCacheByChanges(changes: List[String]): Unit = {
    changes.foreach { change =>
      val ck = CacheKey(change)
      WhiskActionMetaData.removeId(ck)
      WhiskAction.removeId(ck)
      WhiskPackage.removeId(ck)
      WhiskRule.removeId(ck)
      WhiskTrigger.removeId(ck)
      logging.debug(this, s"removed key $ck from cache")
    }
  }

  private def getChanges(limit: Int, pages: Int): Future[List[String]] = {
    dbChanges.getChanges(limit).andThen {
      case Success(changes) => {
        removeFromLocalCacheByChanges(changes)
        if (changes.length == limit && pages > 0) {
          logging.info(this, s"fetched max($limit) changes ($pages pages left)")
          getChanges(limit, pages - 1)
        }
      }
      // no need to swallow exception here as non-fatal exceptions are caught by the scheduler
      case Failure(t) => logging.error(this, s"error on get changes: ${t.getMessage}")
    }
  }

  def scheduleCacheInvalidation(): Any = {
    if (cacheInvalidationEnabled) {
      Scheduler.scheduleWaitAtLeast(
        interval = FiniteDuration(cacheInvalidationPollInterval, TimeUnit.SECONDS),
        initialDelay = FiniteDuration(cacheInvalidationInitDelay, TimeUnit.SECONDS),
        name = "CacheInvalidation") { () =>
        getChanges(cacheInvalidationPageSize, cacheInvalidationMaxPages - 1)
      }
    }
  }

  def ensureInitialSequence(): Future[Unit] = {
    if (cacheInvalidationEnabled) {
      dbChanges.ensureInitialSequence()
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
