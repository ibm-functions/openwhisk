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

package org.apache.openwhisk.core.entity

import java.util.Base64

import org.apache.openwhisk.common.{Logging, PrintStreamLogging, TransactionId}
import org.apache.openwhisk.core.database.{
  MultipleReadersSingleWriterCache,
  NoDocumentException,
  StaleParameter,
  WriteTime
}
import org.apache.openwhisk.core.entitlement.Privilege
import org.apache.openwhisk.core.entity.types.AuthStore
import org.apache.openwhisk.utils.CryptHelpers
import pureconfig._
import pureconfig.generic.auto._
import spray.json._

import scala.concurrent.Future
import scala.util.Try

final case class CRNConfig(environment: String, region: String)

final case class CryptConfig(delimiter: Option[String] = None,
                             version: Option[String] = None,
                             keki: Option[String] = None,
                             kek: Option[String] = None,
                             kekif: Option[String] = None,
                             kekf: Option[String] = None)

case class UserLimits(invocationsPerMinute: Option[Int] = None,
                      concurrentInvocations: Option[Int] = None,
                      firesPerMinute: Option[Int] = None,
                      allowedKinds: Option[Set[String]] = None,
                      storeActivations: Option[Boolean] = None)

object UserLimits extends DefaultJsonProtocol {
  val standardUserLimits = UserLimits()

  implicit val serdes = jsonFormat5(UserLimits.apply)
}

protected[core] case class Namespace(name: EntityName, uuid: UUID)

protected[core] object Namespace extends DefaultJsonProtocol {
  implicit val serdes = jsonFormat2(Namespace.apply)
}

protected[core] case class Identity(subject: Subject,
                                    namespace: Namespace,
                                    authkey: GenericAuthKey,
                                    rights: Set[Privilege] = Set.empty,
                                    limits: UserLimits = UserLimits.standardUserLimits)

object Identity extends MultipleReadersSingleWriterCache[Option[Identity], DocInfo] with DefaultJsonProtocol {

  implicit val logger: Logging = new PrintStreamLogging()

  private val blueAuthConfigNamespace = "whisk.blueauth"
  private val crnConfig = loadConfig[CRNConfig](blueAuthConfigNamespace).toOption
  private val environment = crnConfig.map(_.environment).getOrElse("<environment>")
  private val region = crnConfig.map(_.region).getOrElse("<region>")

  private val cryptConfigNamespace = "whisk.crypt"
  private val cryptConfig = loadConfig[CryptConfig](cryptConfigNamespace).toOption
  private val ccdelim = cryptConfig.map(_.delimiter.getOrElse("::")).getOrElse("::")
  private val ccversion = cryptConfig.map(_.version.getOrElse("CFNv1")).getOrElse("CFNv1")
  private val cckeki = cryptConfig.map(_.keki.getOrElse("")).getOrElse("")
  private val cckek = cryptConfig.map(_.kek.getOrElse("")).getOrElse("")
  private val cckekif = cryptConfig.map(_.kekif.getOrElse("")).getOrElse("")
  private val cckekf = cryptConfig.map(_.kekf.getOrElse("")).getOrElse("")
  logger.info(
    this,
    s"ccdelim: $ccdelim (${ccdelim.length}), " +
      s"ccversion: $ccversion (${ccversion.length}), " +
      s"cckeki: $cckeki (${cckeki.length}), " +
      s"cckek: ${if (cckek.length == 0) cckek else cckek.substring(0, 1)}.. (${cckek.length}), " +
      s"cckekif: $cckekif (${cckekif.length}), " +
      s"cckekf: ${if (cckekf.length == 0) cckekf else cckekf.substring(0, 1)}.. (${cckekf.length})")

  private val viewName = WhiskQueries.view(WhiskQueries.dbConfig.subjectsDdoc, "identities").name

  override val cacheEnabled = true
  override val evictionPolicy = WriteTime
  // upper bound for the auth cache to prevent memory pollution by sending
  // malicious namespace patterns
  override val fixedCacheSize = 100000

  implicit val serdes = jsonFormat5(Identity.apply)

  /**
   * Retrieves a key for namespace.
   * There may be more than one key for the namespace, in which case,
   * one is picked arbitrarily.
   */
  def get(datastore: AuthStore, namespace: EntityName)(implicit transid: TransactionId): Future[Identity] = {
    implicit val logger: Logging = datastore.logging
    implicit val ec = datastore.executionContext
    val ns = namespace.asString
    val nsckey = CacheKey(namespace)

    cacheLookup(
      nsckey, {
        list(datastore, List(ns), limit = 1) map {
          list =>
            list.length match {
              case 1 =>
                val keyFromDb = list.head.fields("value").convertTo[JsObject].fields("key").convertTo[String]
                (keyFromDb.split(ccdelim).toList match {
                  case _ :: version :: keki :: crypttext :: Nil =>
                    keki match {
                      case _ if keki == cckeki =>
                        Try(CryptHelpers.decryptString(crypttext, cckek)).toEither
                      case _ if keki == cckekif =>
                        Try(CryptHelpers.decryptString(crypttext, cckekf)).toEither
                      case _ =>
                        logger.error(this, s"invalid keki $keki, have: ($cckeki,$cckekif)")
                        Left(new IllegalStateException("namespace key not valid"))
                    }
                  case _ =>
                    Right(keyFromDb)
                }) match {
                  case Right(key) =>
                    key.toString.split(':').toList match {
                      case keyuuid :: keykey :: Nil =>
                        Some(rowToIdentity(list.head, keykey, keyFromDb, ns))
                      case _ =>
                        Some(rowToIdentity(list.head, keyFromDb, "", ns))
                    }
                  case Left(e) =>
                    logger
                      .error(
                        this,
                        s"failed to read key of namespace $namespace" +
                          s" using either keki $cckeki or kekif $cckekif " +
                          s"because of ${e.getClass.getSimpleName}: ${e.getMessage}")
                    throw e
                }
              case 0 =>
                logger.info(this, s"$viewName[$namespace] does not exist")
                None
              case _ =>
                logger.error(this, s"$viewName[$namespace] is not unique")
                throw new IllegalStateException("namespace is not unique")
            }
        }
      }).map(_.getOrElse(throw new NoDocumentException("namespace does not exist")))
  }

  private def lookupAuthKeyInCacheOrDatastore(datastore: AuthStore,
                                              authkey: BasicAuthenticationAuthKey,
                                              keyenc: String = "",
                                              keyenctarget: String = "")(implicit transid: TransactionId) = {
    implicit val logger: Logging = datastore.logging
    implicit val ec = datastore.executionContext

    val authkeyForLookup =
      if (keyenc.length == 0) authkey
      else BasicAuthenticationAuthKey(UUID(authkey.uuid.toString), Secret(keyenc))

    cacheLookup(
      CacheKey(authkeyForLookup), {
        list(datastore, List(authkeyForLookup.uuid.asString, authkeyForLookup.key.asString)) map {
          list =>
            list.length match {
              case 1 =>
                Some(
                  rowToIdentity(
                    list.head,
                    authkey.key.key,
                    if (keyenc.length > 0) keyenc else keyenctarget,
                    authkey.uuid.asString))
              case 0 =>
                val len = authkey.key.key.length
                logger.info(
                  this,
                  s"$viewName[spaceguid:${authkey.uuid}, userkey:${authkey.key.key
                    .substring(0, if (len > 1) 2 else len)}..] does not exist, user might have been deleted")
                None
              case _ =>
                val len = authkey.key.key.length
                logger.error(this, s"$viewName[spaceguid:${authkey.uuid}, userkey:${authkey.key.key
                  .substring(0, if (len > 1) 2 else len)}..] is not unique")
                throw new IllegalStateException("uuid is not unique")
            }
        }
      })

  }

  private def lookupAuthKey(datastore: AuthStore, authkey: BasicAuthenticationAuthKey, keyenc: String, keyencf: String)(
    implicit transid: TransactionId) = {
    implicit val logger: Logging = datastore.logging
    implicit val ec = datastore.executionContext

    lookupAuthKeyInCacheOrDatastore(datastore, authkey, keyenc)
      .flatMap {
        case None if (keyencf.length > 0) =>
          // use second key as fallback
          lookupAuthKeyInCacheOrDatastore(datastore, authkey, keyencf)
        case None if (keyenc.length > 0) =>
          // use unencrypted key as fallback
          lookupAuthKeyInCacheOrDatastore(datastore, authkey, "", keyenc)
        case identity =>
          Future.successful(identity)
      }
      .map(_.getOrElse(throw new NoDocumentException("namespace does not exist")))
  }

  def get(datastore: AuthStore, authkey: BasicAuthenticationAuthKey)(
    implicit transid: TransactionId): Future[Identity] = {
    implicit val logger: Logging = datastore.logging
    implicit val ec = datastore.executionContext

    (
      Try(
        if (cckeki.length == 0) ""
        else
          CryptHelpers.encryptString(s"${authkey.uuid}:${authkey.key}", cckek, authkey.uuid.asString)).toEither,
      Try(
        if (cckekif.length == 0) ""
        else
          CryptHelpers
            .encryptString(s"${authkey.uuid}:${authkey.key}", cckekf, authkey.uuid.asString)).toEither) match {
      case (Left(e), _) =>
        val len = authkey.key.key.length
        logger.error(
          this,
          s"failed to read $viewName[spaceguid:${authkey.uuid}, userkey:${authkey.key.key
            .substring(0, if (len > 0) 1 else len)}..] using keki $cckeki " +
            s"because of ${e.getClass.getSimpleName}: ${e.getMessage}")
        throw e
      case (_, Left(e)) =>
        val len = authkey.key.key.length
        logger.error(
          this,
          s"failed to read $viewName[spaceguid:${authkey.uuid}, userkey:${authkey.key.key
            .substring(0, if (len > 0) 1 else len)}..] using keki $cckekif " +
            s"because of ${e.getClass.getSimpleName}: ${e.getMessage}")
        throw e
      case (Right(keyenc), Right(keyencf)) =>
        lookupAuthKey(
          datastore,
          authkey,
          if (keyenc.isEmpty) "" else s"$ccdelim$ccversion$ccdelim$cckeki$ccdelim$keyenc",
          if (keyencf.isEmpty) "" else s"$ccdelim$ccversion$ccdelim$cckekif$ccdelim$keyencf")
    }
  }

  def list(datastore: AuthStore, key: List[Any], limit: Int = 2)(
    implicit transid: TransactionId): Future[List[JsObject]] = {
    datastore.query(
      viewName,
      startKey = key,
      endKey = key,
      skip = 0,
      limit = limit,
      includeDocs = true,
      descending = true,
      reduce = false,
      stale = StaleParameter.No)
  }

  protected[entity] def rowToIdentity(row: JsObject, key: String, keyenc: String, uuidOrNamespace: String)(
    implicit transid: TransactionId,
    logger: Logging) = {
    row.getFields("id", "value", "doc") match {
      case Seq(JsString(id), JsObject(value), doc) =>
        val limits =
          if (doc != JsNull) Try(doc.convertTo[UserLimits]).getOrElse(UserLimits.standardUserLimits)
          else UserLimits.standardUserLimits
        val subject = Subject(id)
        val JsString(uuid) = value("uuid")
        val JsString(namespace) = value("namespace")
        val JsString(account) = JsObject(value).fields.get("account").getOrElse(JsString.empty)
        val crn =
          if (account.isEmpty) ""
          else s"crn:v1:${environment}:public:functions:${region}:a/${account}:s-${uuid}::"
        val crnEncoded = if (crn.isEmpty) "" else Base64.getEncoder.encodeToString(crn.getBytes)

        Identity(
          subject,
          Namespace(EntityName(namespace), UUID(uuid)),
          BasicAuthenticationAuthKey(UUID(uuid), Secret(key), keyenc, crnEncoded, account),
          Privilege.ALL,
          limits)
      case _ =>
        logger.error(this, s"$viewName[$uuidOrNamespace] has malformed view '${row.compactPrint}'")
        throw new IllegalStateException("identities view malformed")
    }
  }
}
