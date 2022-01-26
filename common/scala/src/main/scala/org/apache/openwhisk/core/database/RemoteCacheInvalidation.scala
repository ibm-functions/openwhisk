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

//import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
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

  /**
   * Retry an operation 'step()' awaiting its result up to 'timeout'.
   * Attempt the operation up to 'count' times. The future from the
   * step is not aborted --- TODO fix this.
   */
  /*def retry[T](step: () => Future[T],
               timeout: Duration,
               count: Int = 100,
               graceBeforeRetry: FiniteDuration = 50.milliseconds): Try[T] = {
    val future = step()
    if (count > 0) try {
      val result = Await.result(future, timeout)
      Success(result)
    } catch {
      case n: NoDocumentException =>
        println("no document exception, retrying")
        Thread.sleep(graceBeforeRetry.toMillis)
        retry(step, timeout, count - 1, graceBeforeRetry)
      case RetryOp() =>
        println("condition not met, retrying")
        Thread.sleep(graceBeforeRetry.toMillis)
        retry(step, timeout, count - 1, graceBeforeRetry)
      case t: TimeoutException =>
        println("timed out, retrying")
        Thread.sleep(graceBeforeRetry.toMillis)
        retry(step, timeout, count - 1, graceBeforeRetry)
      case t: Throwable =>
        println(s"unexpected failure $t")
        Failure(t)
    } else Failure(new NoDocumentException("timed out"))
  }*/

  case class PaginationException(message: String) extends Exception(message)
  case class PagingOp() extends Throwable

  def paging2[T](step: () => Future[T]): Try[T] = {
    val future = step()
    try {
      val result = Await.result(future, 5.seconds)
      Success(result)
    } catch {
      case PagingOp() =>
        println("condition not met, retrying")
        paging2(step)
      case t: Throwable =>
        println(s"unexpected failure $t")
        Failure(t)
    }
  }

  def paging3[T](fn: => Future[String]): Future[String] = {
    logging.info(this, s"@StR inside paging function..")
    Try(fn).recover {
      case PagingOp() =>
        logging.error(this, s"@StR caught PagingOp() in paging function")
        paging(fn)
      case t: Throwable =>
        logging.error(this, s"@StR caught t: ${t.getMessage}")
        throw t
    }.get
  }

  def paging[T](fn: => Future[String]): Future[String] = {
    logging.info(this, s"@StR inside paging function..")
    try fn
    catch {
      case PagingOp() =>
        logging.error(this, s"@StR caught PagingOp() in paging function")
        paging(fn)
      case t: Throwable =>
        logging.error(this, s"@StR caught t: ${t.getMessage}")
        throw t
    }
  }

  private def removeFromLocalCacheBySeqs(seqs: List[JsObject]) = {
    logging.info(
      this,
      s"@StR found ${seqs.length} changes (${seqs.filter(_.fields.contains("deleted")).length} deletions)")
    if (seqs.length > 0) {
      logging.info(
        this,
        s"@StR cache before invalidation: " +
          s"actmetasize: ${WhiskActionMetaData.cacheSize}, " +
          s"actsize: ${WhiskAction.cacheSize}, " +
          s"pkgsize: ${WhiskPackage.cacheSize}, " +
          s"rulesize: ${WhiskRule.cacheSize}, " +
          s"trgsize: ${WhiskTrigger.cacheSize}")

      seqs.foreach { seq =>
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
  }

  //@tailrec
  private def getLastChangeUpdateSequences(limit: Int = 1): Future[Unit] = {
    //paging3({
    require(limit >= 0, "limit should be non negative")
    dbClient
      .changes()(since = Some(lcus), limit = Some(limit), descending = false)
      .map {
        case Right(resp) =>
          val newlcus = resp.fields(lcuskey).asInstanceOf[JsString].convertTo[String]
          logging.info(this, s"@StR lcus: $lcus, newlcus: $newlcus")
          val seqs = resp.fields("results").convertTo[List[JsObject]]
          removeFromLocalCacheBySeqs(seqs)

          lcus = newlcus
          logging.info(this, s"@StR new last change update sequence: $lcus")

          if (seqs.length == limit) {
            logging.warn(this, s"@StR fetched maximum limit of $limit changes from db")
            getLastChangeUpdateSequences(limit)
          }

        /*seqs.length match {
            case l if l == limit =>
              logging.warn(this, s"@StR fetched maximum limit of $limit changes from db")
              //throw PagingOp()
              getLastChangeUpdateSequences(limit, newlcus)
            //Future.successful(())
            //lcus
            case _ =>
              val foo = Future.successful(())
              foo
            //lcus
          }*/
        case Left(code) =>
          logging.error(this, s"Unexpected http response code: $code, keep old lcus '$lcus'")
          Future.successful(())
        //lcus
      }
      .recoverWith {
        /*case PagingOp() =>
            logging.error(this, s"@StR caught PagingOp() in recoverWith block")
            throw PagingOp()*/
        case t: Throwable =>
          logging.error(
            this,
            s"@StR '${dbConfig.databaseFor[WhiskEntity]}' internal error for _changes call, failure: '${t.getMessage}', keep old lcus '$lcus'")
          Future.successful(())
      }
      .mapTo[Unit]
    //})
  }

  def scheduleCacheInvalidation() = {
    Scheduler.scheduleWaitAtLeast(interval = 15.seconds, initialDelay = 60.seconds, name = "CacheInvalidation") { () =>
      getLastChangeUpdateSequences(10)
    }
  }

  def ensureInitialLastChangeUpdateSequence() = {

    dbClient
      .changes()(limit = Some(1), descending = true)
      .map {
        case Right(response) =>
          lcus = response.fields(lcuskey).asInstanceOf[JsString].convertTo[String]
          assert(!lcus.isEmpty, s"invalid initial last change update sequence in response: '$response'")
          logging.info(this, s"@StR initial last change update sequence: $lcus")
        //lcus
        case Left(code) =>
          val msg = s"@StR '${dbConfig.databaseFor[WhiskEntity]}' unexecpted response code: $code from _changes call"
          logging.error(this, msg)
          throw new Throwable(msg)
      }
      .recoverWith {
        case t =>
          val msg = s"@StR '${dbConfig.databaseFor[WhiskEntity]}' internal error, failure: '${t.getMessage}'"
          logging.error(this, msg)
          throw t
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
