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

package org.apache.openwhisk.http

import java.nio.file.Paths

import scala.util.Try
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import kamon.Kamon
import pureconfig._
import pureconfig.generic.auto._

import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.FileStorage
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.entity.{
  ActivityEvent,
  ActivityUtils,
  ApiMatcherResult,
  Initiator,
  InitiatorCredential,
  InitiatorHost,
  Reason,
  RequestData,
  Target
}

import scala.concurrent.Future

/**
 * Base class for activity trackers that are called in BasicHttpService on the request path (requestHandler),
 * and on the response path (responseHandler). The method isActive determines whether the activity tracker
 * should used. If isActive returns false then both requestHandler and responseHandlerAsync are not called.
 *
 * The Activity Tracker is instantiated by the Controller class. The related instance is stored in the companion
 * object of BasicHttpService. The Activity Tracker is not (yet) loaded as a SPI.
 *
 * @param actorSystem actor system
 * @param materializer materializer
 * @param logging logging
 * @param logPath path for the activity logs
 */
abstract class AbstractActivityTracker(actorSystem: ActorSystem,
                                       materializer: ActorMaterializer,
                                       logging: Logging,
                                       logPath: String) {

  /** requestHandler is called before each request is processed. It collects data that is required for
   * creating activity events. No further processing should be performed in requestHandler. requestHandler
   * should be fast, incoming requests should not be slowed down. No separate future is created for the
   * requestHandler. Should a requestHandler have to contain long running parts then these parts
   * should run asynchronously (which in turn might have to be synchronized with responseHandlerAsync).
   *
   * @param transid transaction id
   * @param req incoming http request
   */
  def requestHandler(transid: TransactionId, req: HttpRequest): Unit

  /**
   * responseHandlerAsync creates activity events based on the collected data and writes the events to the
   * activity log. responseHandlerAsync is considered a to be little more time consuming and is therefore
   * defined as a future in order to not slow down the response for the API caller.
   *
   * @param transid transaction id
   * @param resp outgoing http response
   * @return
   */
  def responseHandlerAsync(transid: TransactionId, resp: HttpResponse): Future[Unit]

  /**
   * Check if requestHandler and responseHandler should be called (example: create activity logs
   * for the crudcontroller only).
   *
   * @return true if requestHandler and responseHandler should be called. Returns false, otherwise.
   */
  def isActive: Boolean = false
}

// ActivityTracker Implementation
//
// The following implementation creates activity events that are based on the CADF
// standard https://www.dmtf.org/sites/default/files/standards/documents/DSP0262_1.0.0.pdf
// The implementation adds some more information in addition to the CADF standard, see
// https://test.cloud.ibm.com/docs/Activity-Tracker-with-LogDNA?topic=Activity-Tracker-with-LogDNA-ibm_event_fields#eventTime
// The implementation works for both BasicAuthenticationDirective and IAMAuthenticationDirective.
// IAMAuthenticationDirective is an external component from IBM. It it not required to bind this
// component or other external components to openwhisk in order to run the activity tracker implementation
// with BasicAuthenticationDirective.

/**
 * Configuration for the activity tracker implementation.
 *
 * @param runActivityTracker flag that decides whether the acitivity tracker is active or inactive.
 */
case class ActivityTrackerConfig(runActivityTracker: Boolean)

/**
 * The Activity Tracker collects information about user activities and stores the data in activity log files
 * which in turn are sent to a region-specific Activity Tracker LogDNA instance. Users can also inspect
 * their own activities if they instantiate a user Activity Tracker LogDNA instance in their account and in
 * the same region as their cloud function namespace(s).
 *
 * The collected data comprises information about the initiator, the activity, the target service, and the outcome.
 * The entries in the activity log files must follow a specific schema:
 * https://github.ibm.com/activity-tracker/helloATv3/blob/master/eventLinter/events_schema_logdna.json
 *
 * Data collection details:
 *
 * - used tags and where these are set:
 * tagGrantType: BasicAuthenticationDirective || IAMAuthenticationDirective(*)
 * tagHttpMethod: ActivityTracker
 * tagInitiatorId: BasicAuthenticationDirective || IAMAuthenticationDirective
 * tagInitiatorIp: ActivityTracker
 * tagInitiatorName: BasicAuthenticationDirective || IAMAuthenticationDirective
 * tagNamespaceId: ActivityTracker || IAMAuthenticationDirective
 * tagRequestedStatus: Rules (empty, otherwise)
 * tagResourceGroupId: IAMAuthenticationDirective
 * tagTargetId: ActivityTracker || IAMAuthenticationDirective
 * tagTargetIdEncoded: BasicAuthenticationDirective
 * tagUri: ActivityTracker
 * tagUserAgent: ActivityTracker
 *
 * - other collected data:
 * reasoncode: from response
 * eventTime: ActivityEvent
 * id (transid): responseHandler
 *
 * (*) IAMAuthenticationDirective is an external component from IBM. It is not required to bind this component
 * or other external components to openwhisk for running ActivityTracker with BasicAuthenticationDirective.
 *
 * @param actorSystem actor system
 * @param materializer materializer
 * @param logging logging
 * @param logPath path for the activity logs
 */
class ActivityTracker(actorSystem: ActorSystem,
                      materializer: ActorMaterializer,
                      logging: Logging,
                      logPath: String = "/atlogs")
    extends AbstractActivityTracker(actorSystem, materializer, logging, logPath)
    with ActivityUtils {

  private val fileStore = new FileStorage("activitylogs", Paths.get(logPath), materializer, logging)
  private val isCrudController = Kamon.environment.host.toLowerCase.contains("crudcontroller")
  private val config = loadConfig[ActivityTrackerConfig](ConfigKeys.controller).toOption
  private val runActivityTracker = config.exists(_.runActivityTracker)

  def store(line: String): Unit = fileStore.store(line)

  override def isActive: Boolean = runActivityTracker

  logging.info(this, "Activity Tracker instantiated, isActive=" + isActive)

  /**
   * Collects data from the request for the activity tracker. Data are stored in transid meta tags
   * with restricted length for later usage in responseHandler. Collected data:
   * - http method
   * - url
   * - values of headers x-forwarded-for and user-agent
   *
   * @param transid transaction id
   * @param req api request
   */
  def requestHandler(transid: TransactionId, req: HttpRequest): Unit = {
    try {
      transid.setTag(TransactionId.tagHttpMethod, getString(req.method.value, 7))
      transid.setTag(TransactionId.tagUri, getString(req.uri.toString, 2048))

      val it = req.headers.iterator
      var found = 0

      // copy headers x-forwarded-for and user-agent
      while (it.hasNext && found < 2) {
        val hdr = it.next
        val hdrName = hdr.name.toLowerCase
        if (hdrName == "x-forwarded-for") {
          transid.setTag(TransactionId.tagInitiatorIp, getString(hdr.value, 64))
          found += 1
        } else if (hdrName == "user-agent") {
          transid.setTag(TransactionId.tagUserAgent, getString(hdr.value, 350))
          found += 1
        }
      }
    } catch {
      case ex: Exception =>
        logging.error(this, "Unexpected exception caught in requestHandler: " + ex.getMessage)
    }
  }

  def responseHandlerAsync(transid: TransactionId, resp: HttpResponse): Future[Unit] =
    if (isActive) Future { responseHandler(transid, resp) }(actorSystem.dispatcher) else Future.successful((): Unit)

  /**
   * Combines data from the response with data from the request handler and other collected data
   * to create an activity tracker event that is finally written to the activity tracker log file.
   *
   * @param transid transaction id
   * @param resp api repsonse
   */
  private def responseHandler(transid: TransactionId, resp: HttpResponse): Unit = {
    try {
      val initiatorName = transid.getTag(TransactionId.tagInitiatorName)
      val httpMethod = transid.getTag(TransactionId.tagHttpMethod)
      val uri = transid.getTag(TransactionId.tagUri)

      if (!isIgnoredUser(initiatorName)) {

        val serviceAction: ApiMatcherResult = getServiceAction(transid, httpMethod, uri, logging)

        if (serviceAction != null) {

          val initiator =
            Initiator(
              transid.getTag(TransactionId.tagInitiatorId),
              transid.getTag(TransactionId.tagInitiatorName),
              "service/security/account/user",
              InitiatorCredential(getGrantType(transid.getTag(TransactionId.tagGrantType))),
              InitiatorHost(transid.getTag(TransactionId.tagInitiatorIp)))

          val reasonCode = resp.status.value.split(" ")(0)
          val reasonCodeInt = Try { reasonCode.toInt }.getOrElse(0)
          val reasonType = getReasonType(reasonCode)
          val success = reasonCodeInt >= 200 && reasonCodeInt < 300
          val targetId = getTargetId(transid)
          val resourceGroupId = transid.getTag(TransactionId.tagResourceGroupId)
          val scope = extractScope(targetId)

          val resourceGroupCrn =
            if (resourceGroupId == null || resourceGroupId.isEmpty)
              ""
            else
              "crn:v1:bluemix:public:resource-controller:global:" +
                (if (scope.isEmpty) "unknown" else scope) + "::resource-group:" +
                resourceGroupId

          val requestData =
            RequestData(
              method = transid.getTag(TransactionId.tagHttpMethod),
              url = uri,
              userAgent = transid.getTag(TransactionId.tagUserAgent),
              failure = if (success) "" else reasonType,
              resourceGroupCrn = resourceGroupCrn)

          var nameSpaceId = transid.getTag(TransactionId.tagNamespaceId)
          if (nameSpaceId.isEmpty) nameSpaceId = extractInstance(targetId)

          val logMessage = serviceAction.logMessage +
            (if (nameSpaceId == "") "" else " for namespace " + nameSpaceId) +
            (if (success) "" else " -failure")

          val event = ActivityEvent(
            initiator = initiator,
            target = Target(targetId, serviceAction.targetName, serviceAction.targetType),
            action = serviceAction.actionType,
            outcome = if (success) "success" else "failure",
            reason = Reason(reasonCode, reasonType),
            severity = "warning",
            message = logMessage,
            logSourceCRN = convertToLogSourceCRN(targetId),
            saveServiceCopy = true,
            dataEvent = serviceAction.isDataEvent,
            id = transid.toString,
            requestData)

          val line = event.toJson.compactPrint
          logging.info(this, "activity tracker event: " + line.replaceAll("\\{", "(").replaceAll("\\}", ")"))(
            id = transid)

          // write to activity log
          store(line)
        }
      }

    } catch {
      case ex: Exception =>
        logging.error(this, "Unexpected exception caught in responseHandler: " + ex.getMessage)
    }

  }
}
