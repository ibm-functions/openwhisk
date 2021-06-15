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

package org.apache.openwhisk.core.controller

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.{AuthenticationDirective, AuthenticationResult}
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, Scheduler, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.database.NoDocumentException
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.types.AuthStore
import org.apache.openwhisk.core.invoker.{NamespaceBlacklist, NamespaceBlacklistConfig}
import pureconfig.loadConfigOrThrow
import pureconfig.generic.auto._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import spray.json.JsString

object BasicAuthenticationDirective extends AuthenticationDirectiveProvider {

  var namespaceBlacklist: Option[NamespaceBlacklist] = None
  def getOrCreateBlacklist()(implicit transid: TransactionId,
                             system: ActorSystem,
                             ec: ExecutionContext,
                             logging: Logging): NamespaceBlacklist = {
    val blacklist =
      if (namespaceBlacklist.isDefined) namespaceBlacklist.get
      else {
        logging.info(this, s"controller name: ${sys.env.get("CONTROLLER_NAME").getOrElse("")}")
        logging.info(this, "create blacklist..")
        val authStore = WhiskAuthStore.datastore()(system, logging, ActorMaterializer())
        val namespaceBlacklist = new NamespaceBlacklist(authStore)
        if (!sys.env.get("CONTROLLER_NAME").getOrElse("").equals("crudcontroller")) {
          logging.info(this, "create background job to update blacklist..")
          Scheduler.scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) {
            () =>
              logging.debug(this, "running background job to update blacklist")
              namespaceBlacklist.refreshBlacklist()(authStore.executionContext, transid).andThen {
                case Success(set) => {
                  logging.info(
                    this,
                    s"updated blacklist to ${set.size} items (accounts: ${set.filter(i => i matches "[0-9a-z]{32}")})")
                }
                case Failure(t) => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
              }
          }
        }
        namespaceBlacklist
      }
    namespaceBlacklist = Some(blacklist)
    blacklist
  }

  def validateCredentials(credentials: Option[BasicHttpCredentials])(implicit transid: TransactionId,
                                                                     system: ActorSystem,
                                                                     ec: ExecutionContext,
                                                                     logging: Logging,
                                                                     authStore: AuthStore): Future[Option[Identity]] = {
    credentials flatMap { pw =>
      Try {
        // authkey deserialization is wrapped in a try to guard against malformed values
        val authkey = BasicAuthenticationAuthKey(UUID(pw.username), Secret(pw.password))
        logging.info(this, s"authenticate: ${authkey.uuid}")
        val future = Identity.get(authStore, authkey) map { result =>
          val blacklist = getOrCreateBlacklist
          val identity =
            if (!blacklist.isEmpty && blacklist.isBlacklisted(
                  Try(result.authkey.asInstanceOf[BasicAuthenticationAuthKey].account).getOrElse(""))) {
              Identity(
                subject = result.subject,
                namespace = result.namespace,
                authkey = result.authkey,
                rights = result.rights,
                limits =
                  UserLimits(invocationsPerMinute = Some(0), concurrentInvocations = Some(0), firesPerMinute = Some(0)))
            } else result

          // store info for activity tracker
          val name = identity.subject.asString
          transid.setTag(TransactionId.tagNamespaceId, identity.namespace.name.asString)
          transid.setTag(TransactionId.tagInitiatorId, name)
          transid.setTag(TransactionId.tagInitiatorName, name)
          transid.setTag(TransactionId.tagGrantType, "password")
          val JsString(crnEncoded) =
            identity.authkey.toEnvironment.fields.getOrElse("namespace_crn_encoded", JsString.empty)
          transid.setTag(TransactionId.tagTargetIdEncoded, crnEncoded)

          if (authkey == identity.authkey) {
            logging.debug(this, s"authentication valid")
            Some(identity)
          } else {
            logging.debug(this, s"authentication not valid")
            None
          }
        } recover {
          case _: NoDocumentException | _: IllegalArgumentException =>
            logging.debug(this, s"authentication not valid")
            None
        }
        future.failed.foreach(t => logging.error(this, s"authentication error: $t"))
        future
      }.toOption
    } getOrElse {
      credentials.foreach(_ => logging.debug(this, s"credentials are malformed"))
      Future.successful(None)
    }
  }

  /** Creates HTTP BasicAuth handler */
  def basicAuth[A](verify: Option[BasicHttpCredentials] => Future[Option[A]]): AuthenticationDirective[A] = {
    extractExecutionContext.flatMap { implicit ec =>
      authenticateOrRejectWithChallenge[BasicHttpCredentials, A] { creds =>
        verify(creds).map {
          case Some(t) => AuthenticationResult.success(t)
          case None    => AuthenticationResult.failWithChallenge(HttpChallenges.basic("OpenWhisk secure realm"))
        }
      }
    }
  }

  def identityByNamespace(
    namespace: EntityName)(implicit transid: TransactionId, system: ActorSystem, authStore: AuthStore) = {
    //Identity.get(authStore, namespace)
    implicit val ec = authStore.executionContext
    implicit val logging = authStore.logging
    Identity.get(authStore, namespace) map { result =>
      val blacklist = getOrCreateBlacklist
      if (!blacklist.isEmpty && blacklist.isBlacklisted(
            Try(result.authkey.asInstanceOf[BasicAuthenticationAuthKey].account).getOrElse(""))) {
        Identity(
          subject = result.subject,
          namespace = result.namespace,
          authkey = result.authkey,
          rights = result.rights,
          limits = UserLimits(invocationsPerMinute = Some(0), concurrentInvocations = Some(0), firesPerMinute = Some(0)))
      } else result
    }
  }

  def authenticate(implicit transid: TransactionId,
                   authStore: AuthStore,
                   logging: Logging): AuthenticationDirective[Identity] = {
    extractActorSystem.flatMap { implicit system =>
      extractExecutionContext.flatMap { implicit ec =>
        basicAuth(validateCredentials)
      }
    }
  }
}
