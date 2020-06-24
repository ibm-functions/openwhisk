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

import java.text.SimpleDateFormat

import org.apache.openwhisk.common.{Logging, TransactionId}

import scala.collection.immutable
import spray.json.{JsBoolean, JsNumber, JsObject, JsString}
import java.net.URL
import java.util.Base64

import scala.util.matching.Regex
import scala.util.Try

/**
 * Activity Tracker event definition and json serialization
 *
 * @param logSourceCRN logDNA target crn
 * @param saveServiceCopy logDNA flag for storing a copy in the cloud functions system log
 */
case class ActivityEvent(initiator: Initiator,
                         target: Target,
                         action: String,
                         outcome: String,
                         reason: Reason,
                         severity: String,
                         message: String,
                         logSourceCRN: String,
                         saveServiceCopy: Boolean,
                         dataEvent: Boolean,
                         id: String,
                         requestData: RequestData)
    extends ActivityUtils {

  private def eventTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS+0000")

  def toJson =
    JsObject(
      Map(
        "initiator" -> initiator.toJson,
        "target" -> target.toJson,
        "action" -> getJsString(action),
        "outcome" -> getJsString(outcome),
        "reason" -> reason.toJson,
        "severity" -> getJsString(severity), // normal, warning, critical
        "eventTime" -> getJsString(eventTimeFormatter.format(System.currentTimeMillis)),
        "message" -> getJsString(message),
        "logSourceCRN" -> getJsString(logSourceCRN),
        "saveServiceCopy" -> JsBoolean(saveServiceCopy),
        "observer" -> Observer().toJson,
        "dataEvent" -> JsBoolean(dataEvent),
        "id" -> getJsString(id),
        "requestData" -> requestData.toJson,
        "responseData" -> JsObject()))
}

case class RequestData(method: String, url: String, userAgent: String, failure: String, resourceGroupCrn: String)
    extends ActivityUtils {
  def toJson =
    JsObject(
      if (failure.isEmpty)
        Map(
          "method" -> getJsString(method),
          "url" -> getJsString(url),
          "userAgent" -> getJsString(userAgent),
          "resourceGroupId" -> getJsString(resourceGroupCrn)) // misleading field name, resourceGroupId is a CRN
      else
        Map(
          "method" -> getJsString(method),
          "url" -> getJsString(url),
          "userAgent" -> getJsString(userAgent),
          "reasonForFailure" -> getJsString(failure),
          "resourceGroupId" -> getJsString(resourceGroupCrn))) // misleading field name, resourceGroupId is a CRN
}

case class Observer() extends ActivityUtils {
  def toJson = JsObject(Map("name" -> getJsString("ActivityTracker")))
}

case class Target(id: String, name: String, typeURI: String) extends ActivityUtils {
  def toJson =
    JsObject(Map("id" -> getJsString(id), "name" -> getJsString(name), "typeURI" -> getJsString(typeURI)))
}

case class TargetHost(address: String) extends ActivityUtils {
  def toJson = JsObject(Map("address" -> getJsString(address)))
}

case class Initiator(id: String, name: String, typeURI: String, credential: InitiatorCredential, host: InitiatorHost)
    extends ActivityUtils {
  def toJson =
    JsObject(
      Map(
        "id" -> getJsString(id),
        "name" -> getJsString(name),
        "typeURI" -> getJsString(typeURI),
        "credential" -> credential.toJson,
        "host" -> host.toJson))
}

// agent is no official field but contains helpful information
case class InitiatorHost(address: String) extends ActivityUtils {
  def toJson = JsObject(Map("address" -> getJsString(address)))
}

case class InitiatorCredential(typeURI: String) extends ActivityUtils {
  def toJson = JsObject(Map("type" -> getJsString(typeURI)))
}

case class Reason(reasonCode: String, reasonType: String) extends ActivityUtils {
  def toJson =
    JsObject(Map("reasonCode" -> getJsNumber(reasonCode), "reasonType" -> getJsString(reasonType)))
}

case class ApiMatcherResult(actionType: String,
                            logMessage: String,
                            targetName: String,
                            targetType: String,
                            isDataEvent: Boolean)

trait ActivityUtils {

  def getJsString(s: String): JsString = { if (s == null) JsString("(null)") else JsString(s) }
  def getJsNumber(s: String): JsNumber = { if (s == null) JsNumber("0") else JsNumber(s) }

  // fast path from reasonCode to short description
  def getReasonType(reasonCode: String): String = reasonTypes.getOrElse(reasonCode, "Unassigned")

  private val reasonTypes = immutable.HashMap(
    "0" -> "Not Set",
    "100" -> "Continue",
    "101" -> "Switching Protocols",
    "102" -> "Processing",
    "103" -> "Early Hints",
    "200" -> "OK",
    "201" -> "Created",
    "202" -> "Accepted",
    "203" -> "Non-Authoritative Information",
    "204" -> "No Content",
    "205" -> "Reset Content",
    "206" -> "Partial Content",
    "207" -> "Multi-Status",
    "208" -> "Already Reported",
    "226" -> "IM Used",
    "300" -> "Multiple Choices",
    "301" -> "Moved Permanently",
    "302" -> "Found",
    "303" -> "See Other",
    "304" -> "Not Modified",
    "305" -> "Use Proxy",
    "307" -> "Temporary Redirect",
    "308" -> "Permanent Redirect",
    "400" -> "Bad Request",
    "401" -> "Unauthorized",
    "402" -> "Payment Required",
    "403" -> "Forbidden",
    "404" -> "Not Found",
    "405" -> "Method Not Allowed",
    "406" -> "Not Acceptable",
    "407" -> "Proxy Authentication Required",
    "408" -> "Request Timeout",
    "409" -> "Conflict",
    "410" -> "Gone",
    "411" -> "Length Required",
    "412" -> "Precondition Failed",
    "413" -> "Payload Too Large",
    "414" -> "URI Too Long",
    "415" -> "Unsupported Media Type",
    "416" -> "Range Not Satisfiable",
    "417" -> "Expectation Failed",
    "421" -> "Misdirected Request",
    "422" -> "Unprocessable Entity",
    "423" -> "Locked",
    "424" -> "Failed Dependency",
    "425" -> "Too Early",
    "426" -> "Upgrade Required",
    "428" -> "Precondition Required",
    "429" -> "Too Many Requests",
    "430" -> "Unassigned",
    "431" -> "Request Header Fields Too Large",
    "451" -> "Unavailable For Legal Reasons",
    "500" -> "Internal Server Error",
    "501" -> "Not Implemented",
    "502" -> "Bad Gateway",
    "503" -> "Service Unavailable",
    "504" -> "Gateway Timeout",
    "505" -> "HTTP Version Not Supported",
    "506" -> "Variant Also Negotiates",
    "507" -> "Insufficient Storage",
    "508" -> "Loop Detected",
    "509" -> "Unassigned",
    "510" -> "Not Extended",
    "511" -> "Network Authentication Required")

  private val ignoredUsers = List("lime@us.ibm.com", "whisk.system")

  def isIgnoredUser(name: String): Boolean = ignoredUsers.contains(name)

  private val grantTypes = immutable.HashMap(
    "urn:ibm:params:oauth:grant-type:apikey" -> "apikey",
    "urn:ibm:params:oauth:grant-type:delegated-refresh-token" -> "token",
    "urn:ibm:params:oauth:grant-type:passcode" -> "user",
    "authorization_code" -> "user",
    "password" -> "user")

  def getGrantType(reasonCode: String): String = grantTypes.getOrElse(reasonCode, "unknown")

  /**
   * Returns the position of the n-th occurrence of a substring within a string.
   *
   * @param str string
   * @param substr substring to search for
   * @param n occurrence number, starting with 1
   * @return position of the n-th occurrence of substr, or -1 if not found
   */
  def posN(str: String, substr: String, n: Int): Int = {
    if (n > 0) {
      if (substr.isEmpty) 0
      else {
        var pos = str.indexOf(substr)
        var i = n - 1
        while (i > 0 && pos != -1) {
          pos = str.indexOf(substr, pos + 1)
          i -= 1
        }
        pos
      }
    } else -1
  }

  /**
   * Reduces string input if it is too long.
   *
   * @param s input string
   * @param maxlen max length allowed
   * @return s reduced to first maxlen chars, or empty string if s is null
   */
  def getString(s: String, maxlen: Int): String = {
    if (s == null) ""
    else if (s.length > maxlen) s.substring(0, maxlen) + "..."
    else s
  }

  // API matchers

  // no audit events for: activations API, action invocations, list all resources of a type

  /**
   * Returns the ApiMatcherResult (= actionType, logMessage, entityName) for the given uri and http method
   * if event should be logged. Returns null, otherwise.
   *
   * @param transid transaction id
   * @param isCrudController indicates whether the code is running as crudcontroller or controller
   * @param httpMethod http method of the request
   * @param uri uri of the request
   * @param logger logger
   * @return instance of ApiMatcherResult, or null if event should not be logged
   */
  def getServiceAction(transid: TransactionId,
                       isCrudController: Boolean,
                       httpMethod: String,
                       uri: String,
                       logger: Logging): ApiMatcherResult = {
    val url = Try { new URL(uri) }.getOrElse(null)
    if (url == null) {
      logger.error(this, "audit.log - invalid or emtpy URL: " + uri)(id = transid)
      null
    } else {
      val urlPath = url.getPath
      if (urlPath == "/ping")
        null
      else {
        val method = if (httpMethod == null) "UNKNOWN" else httpMethod.toUpperCase
        if (isCrudController) // code is running as crudController
          matchAPI(transid, "action", actionsAPIMatcher, method, urlPath) // actions API
            .getOrElse(
              matchAPI(transid, "package", packagesAPIMatcher, method, urlPath) // packages API
                .getOrElse(
                  matchRulesAPI(transid, method, urlPath, logger) // rules API
                    .getOrElse(matchAPI(transid, "trigger", triggersAPIMatcher, method, urlPath) // triggers API
                      .getOrElse(matchOther(transid, method, urlPath, logger).orNull)))) // nothing to add to the log
        else // code is running  as controller (handles POST rule API call for enable/disable rule)
          matchRulesAPI(transid, method, urlPath, logger) // rules API
            .getOrElse(matchOther(transid, method, urlPath, logger).orNull) // nothing to add to the log

      }
    }
  }

  private val thisService = "functions"
  private val messagePrefix = "IBM Cloud Functions: "

  /**
   * Converts a target crn to a logSourceCRN (removes all sections after account scope section)
   * Example:
   * in:  crn:v1:bluemix:public:functions:us-south:a/eb2ee6585c91a27a709a44e2652a381a:94d7acb2-0604-403e-a2bb-eb4d34fbf154::
   * out: crn:v1:bluemix:public:functions:us-south:a/eb2ee6585c91a27a709a44e2652a381a:::
   *
   * @param crn target crn
   * @return logsourceCRN for LogDNA
   */
  def convertToLogSourceCRN(crn: String): String = {
    val endOfAccountPos = posN(crn, ":", 7)
    if (endOfAccountPos > 0) crn.substring(0, endOfAccountPos) + ":::" else crn
  }

  /**
   * Extract service instance segment from crn
   * in:  crn:v1:bluemix:public:functions:us-south:a/eb2ee6585c91a27a709a44e2652a381a:94d7acb2-0604-403e-a2bb-eb4d34fbf154::
   * out: 94d7acb2-0604-403e-a2bb-eb4d34fbf154
   * @param crn target crn
   * @return content of service instance segment or empty string if error
   */
  def extractInstance(crn: String): String = {
    val startOfInstancePos = posN(crn, ":", 7) + 1
    val endOfInstancePos = posN(crn, ":", 8)
    if (startOfInstancePos > 0 && endOfInstancePos > 0) crn.substring(startOfInstancePos, endOfInstancePos) else ""
  }

  /**
   * Extract scope segment from crn
   * in:  crn:v1:bluemix:public:functions:us-south:a/eb2ee6585c91a27a709a44e2652a381a:94d7acb2-0604-403e-a2bb-eb4d34fbf154::
   * out: a/eb2ee6585c91a27a709a44e2652a381a
   * @param crn target crn
   * @return content of scope segment without prefix a/ or empty string if error
   */
  def extractScope(crn: String): String = {
    if (crn == null)
      ""
    else {
      val startOfScopePos = posN(crn, ":", 6) + 1
      val endOfScopePos = posN(crn, ":", 7)
      if (startOfScopePos > 0 && endOfScopePos > 0 && startOfScopePos < endOfScopePos)
        crn.substring(startOfScopePos, endOfScopePos)
      else ""
    }
  }

  /**
   * Retrieve targetId from transaction id meta tags tagTargetIdEncoded and tagTargetId.
   *
   * If tagTargetIdEncoded is defined then basic authentication was applied that stored the base64-encoded targetId
   * in tagTargetIdEncoded. Otherwise, iam token authentication was applied and the targetId is stored in tagTargetId.
   *
   * @param transid TransactionId
   * @return
   */
  def getTargetId(transid: TransactionId): String = {
    val targetIdEncoded = transid.getTag(TransactionId.tagTargetIdEncoded)

    val result = if (targetIdEncoded.isEmpty) {
      transid.getTag(TransactionId.tagTargetId)
    } else
      Try { new String(Base64.getDecoder.decode(targetIdEncoded)) }.getOrElse("")

    if (result == null) "null" else result
  }

  // uri example:
  // https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/hello10?blocking=true&result=false
  private val actionsAPIMatcher = """/api/v1/namespaces/.*/actions/.*""".r
  private val triggersAPIMatcher = """/api/v1/namespaces/.*/triggers/.*""".r
  private val packagesAPIMatcher = """/api/v1/namespaces/.*/packages/.*""".r
  private val rulesAPIMatcher = """/api/v1/namespaces/.*/rules/.*""".r

  /**
   * a matcher used for the APIs of actions, triggers and packages
   *
   * @param transid transaction id
   * @param entityType action, trigger, package
   * @param matcher actionsAPIMatcher, packagesAPIMatcher, triggersAPIMatcher
   * @param method http method of the request
   * @param uri uri of the request
   * @return Some(ApiMatcherResult) or None
   */
  def matchAPI(transid: TransactionId,
               entityType: String,
               matcher: Regex,
               method: String,
               uri: String): Option[ApiMatcherResult] = {
    // ignored: invoke action, list all actions
    val entityTypePathSelector = entityType + "s" // plural of entityType
    val targetType = thisService + "/" + entityType
    val actionTypePrefix = thisService + "." + entityType
    uri match {
      case matcher() =>
        val pos = uri.indexOf("/" + entityTypePathSelector + "/") + ("/" + entityTypePathSelector + "/").length
        val targetName = uri.substring(pos)
        method match {
          case "GET" =>
            Some(
              ApiMatcherResult(
                actionTypePrefix + ".read",
                messagePrefix + "read " + entityType + " " + targetName,
                targetName,
                targetType,
                isDataEvent = true))
          case "PUT" =>
            val isUpdate = !transid.getTag(TransactionId.tagUpdateInfo).isEmpty
            val operation = if (isUpdate) "update" else "create"
            Some(
              ApiMatcherResult(
                actionTypePrefix + "." + operation,
                messagePrefix + operation + " " + entityType + " " + targetName,
                targetName,
                targetType,
                isDataEvent = false))
          case "DELETE" =>
            Some(
              ApiMatcherResult(
                actionTypePrefix + ".delete",
                messagePrefix + "delete " + entityType + " " + targetName,
                targetName,
                targetType,
                isDataEvent = false))
          case _ => None
        }
      case _ =>
        None
    }
  }

  /**
   * Matcher for the rules API. In contrast to other entities the POST method (enable/disable a rule) is logged
   *
   * @param method http method of the request
   * @param uri uri of the request
   * @return Some(ApiMatcherResult) or None
   */
  def matchRulesAPI(transid: TransactionId, method: String, uri: String, logger: Logging): Option[ApiMatcherResult] = {
    // ignored: list all rules
    val targetType = thisService + "/" + "rule"
    uri match {
      case rulesAPIMatcher() =>
        val pos = uri.indexOf("/rules/") + "/rules/".length
        val ruleName = uri.substring(pos)
        method match {
          case "GET" =>
            Some(
              ApiMatcherResult(
                thisService + ".rule.read",
                messagePrefix + "read rule " + ruleName,
                ruleName,
                targetType,
                isDataEvent = true))
          case "PUT" =>
            val isUpdate = !transid.getTag(TransactionId.tagUpdateInfo).isEmpty
            val operation = if (isUpdate) "update" else "create"
            Some(
              ApiMatcherResult(
                thisService + ".rule." + operation,
                messagePrefix + operation + " rule " + ruleName,
                ruleName,
                targetType,
                isDataEvent = false))
          case "DELETE" =>
            Some(
              ApiMatcherResult(
                thisService + ".rule.delete",
                messagePrefix + "delete rule " + ruleName,
                ruleName,
                targetType,
                isDataEvent = false))
          case "POST" =>
            val requestedStatus = transid.getTag(TransactionId.tagRequestedStatus)
            requestedStatus match {
              case "active" =>
                Some(
                  ApiMatcherResult(
                    thisService + ".rule.enable",
                    messagePrefix + "enable rule " + ruleName,
                    ruleName,
                    targetType,
                    isDataEvent = false))
              case "inactive" =>
                Some(
                  ApiMatcherResult(
                    thisService + ".rule.disable",
                    messagePrefix + "disable rule " + ruleName,
                    ruleName,
                    targetType,
                    isDataEvent = false))
              case _ =>
                logger.error(this, "audit.log - rules API, POST: unexpected requested status: " + requestedStatus)(
                  id = transid)
                None
            }
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * This matcher is finally called if there are no other matches in getServiceAction.
   * In other words, no event should be generated for this request (e.g., for invoke action, list all namespaces, etc.).
   *
   * @param method http method of the request
   * @param uri uri of the request
   * @return None
   */
  def matchOther(transactionId: TransactionId,
                 method: String,
                 uri: String,
                 logger: Logging): Option[ApiMatcherResult] = {
    logger.debug(this, "audit log - no event created for method=" + method + ", uri=" + uri)(id = transactionId)
    None
  }

}
