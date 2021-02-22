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

package org.apache.openwhisk.core.controller.test

import java.nio.file.Paths
import java.io.{File, FilenameFilter}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}
import org.scalatestplus.junit.JUnitRunner
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import spray.json._
import DefaultJsonProtocol._
import common.StreamLogging
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.database.FileStorage
import org.apache.openwhisk.http.ActivityTracker
import org.apache.openwhisk.core.entity.ActivityUtils

import scala.concurrent.duration._
import scala.io.Source

// Tags have been added to TransactionId for storing key-value pairs along the path of a request. This is the
// list of the currently used tags for creating activity-tracker log files, and where these tags are set:
//
//  tagGrantType: BasicAuthenticationDirective || IAMAuthenticationDirective
//  tagHttpMethod: ActivityTracker
//  tagInitiatorId: BasicAuthenticationDirective || IAMAuthenticationDirective
//  tagInitiatorIp: ActivityTracker
//  tagInitiatorName: BasicAuthenticationDirective || IAMAuthenticationDirective
//  tagNamespaceId: ActivityTracker || IAMAuthenticationDirective
//  tagRequestedStatus: Rules (empty, otherwise)
//  tagResourceGroupId: IAMAuthenticationDirective
//  tagTargetId: ActivityTracker || IAMAuthenticationDirective
//  tagTargetIdEncoded: BasicAuthenticationDirective
//  tagUri: ActivityTracker
//  tagUserAgent: ActivityTracker
//
// This test includes a simulation of IAMAuthenticationDirective, an external component from IBM.
// It is not required to bind this component to openwhisk.
// The code for activity logs works for BasicAuthenticationDirective and IAMAuthenticationDirective.

@RunWith(classOf[JUnitRunner])
class ActivityTrackerTests()
    extends FlatSpecLike
    with Matchers
    with StreamLogging
    with BeforeAndAfterEach
    with ActivityUtils {

  implicit val actorSystem: ActorSystem = ActorSystem("ActivityTrackerTests")
  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val waitTime = 30.seconds

  private val r = scala.util.Random

  override def beforeEach = stream.reset()

  it should "handle TransactionId tags correctly when using multiple threads" in {

    val transid = TransactionId("testTags")
    val n = 20
    val waitMillis = 6
    val ra = new Array[Int](n)

    for (i <- 0 until n) ra(i) = r.nextInt(waitMillis)

    val set_fs = (0 until n).map(i => {
      Future {
        Thread.sleep(ra(i))
        transid.setTag("name" + i, i.toString)
      }
    })

    Await.result(Future.sequence(set_fs), 60.seconds)

    for (i <- 0 until n) ra(i) = r.nextInt(waitMillis)

    val get_fs = (0 until n).map(i => {
      Future {
        Thread.sleep(ra(i))
        (i, transid.getTag("name" + i))
      }
    })

    Await.result(Future.sequence(get_fs), 60.seconds).map(pair => pair._1.toString shouldBe pair._2)
  }

  it should "verify that FileStorage creates file content correctly when using multiple threads" in {

    // use /tmp instead of default path to ensure sufficient access rights for the file
    val logPath = "/tmp"
    val logFilePrefix = "testFileStorage"
    val logFileMaxSize = 1000000
    val fileStore = new FileStorage(logFilePrefix, logFileMaxSize, Paths.get(logPath), materializer, logging)
    val line = "The quick brown fox jumps over the lazy dog. The five boxing wizards jump quickly."
    val dir = new File(logPath)
    val fileFilter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith(logFilePrefix)
    }

    var foundFiles = dir.listFiles(fileFilter)
    for (f <- foundFiles) f.delete() // not embedded in try/catch - test should fail if there is a problem here

    val n = 20
    val ra = new Array[Int](n)
    val waitMillis = 6

    for (i <- 0 until n) ra(i) = r.nextInt(waitMillis)

    val write_fs = (0 until n).map(i => {
      Future {
        Thread.sleep(ra(i))
        fileStore.store(line)
      }
    })

    Await.result(Future.sequence(write_fs), 60.seconds)

    var remainingAttempts = 3
    var found = false
    while (remainingAttempts > 0 && !found) {
      Thread.sleep(5000)
      foundFiles = dir.listFiles(fileFilter)
      found = foundFiles.length > 0 && foundFiles(0).length > 0
      remainingAttempts -= 1
    }

    foundFiles.length shouldBe 1

    val filePath = foundFiles(0).getAbsolutePath
    val source = Source.fromFile(filePath)
    val content = source.mkString
    source.close

    content shouldBe (line + "\n") * n

  }

  def verifyEvent(resultString: String, expectedString: String): Unit = {
    if (resultString != null || expectedString != null) {
      if (resultString == null) "ok" shouldBe "resultString is null but expectedString is " + expectedString
      if (expectedString == null) "ok" shouldBe "expectedString is null but resultString is " + resultString

      val result = Try { resultString.parseJson }.getOrElse(null)
      if (result == null) "ok" shouldBe "resultString is no valid json: " + resultString

      val resultFields = result.asJsObject.fields
      val expectedFields = expectedString.parseJson.asJsObject.fields
      val expectedFieldsKeys = expectedFields.keySet

      resultFields.keySet shouldBe expectedFieldsKeys

      for (field <- expectedFieldsKeys)
        if (field != "eventTime") resultFields.get(field) shouldBe expectedFields.get(field)

      // expected format: YYY-MM-DDTHH:mm:ss.SS+0000, e.g. "2019-11-03T21:40:53.94+0000" (at least 2 digits for millis)
      val timestamp = resultFields("eventTime").convertTo[String]
      val ok = timestamp.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{2}\\+0{4}")
      "ok" shouldBe (if (ok) "ok" else "timestamp " + timestamp + " does not match YYYY-MM-DDTHH:mm:ss.SS+0000")
    }
  }

  def getExpectedResultForInvalidValuesTests(
    credType: String = "apikey",
    targetId: String =
      "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::",
    logSourceCRN: String = "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
    accountInResourceGroupId: String = "a/eb2e36585c91a27a709c44e2652a381a") =
    s"""
{"requestData":{
    "requestId":"test_get_activation",
    "ruleName" :"testrule",
    "method":"GET",
    "url":"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/testrule"
 },
 "resourceGroupId":"crn:v1:bluemix:public:resource-controller:global:$accountInResourceGroupId::resource-group:ca23a1a3f0a84e2ab6b70c22ec6b1324",
 "observer":{"name":"ActivityTracker"},
 "outcome":"success",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":200,
     "reasonType":"OK"
 },
 "eventTime":"2020-06-04T15:02:20.663+0000",
 "message":"Functions: read rule testrule for namespace a88c0a24-853b-4477-82f8-6876e72bebf2",
 "target":{
     "id":"$targetId",
     "name":"",
     "typeURI":"functions/namespace/rule"
 },
 "severity":"normal",
 "logSourceCRN":"$logSourceCRN",
 "action":"functions.rule.read",
 "initiator":{
    "name":"john.doe@acme.com",
    "host":{
       "address":"192.168.0.1",
       "addressType":"IPv4",
       "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
    },
    "id":"IBMid-310000GN7M",
    "typeURI":"service/security/account/user",
    "credential":{"type":"$credType"}
 },
 "dataEvent":true,
 "responseData":{}
}
"""

  // crudcontroller tests (getIsCrudController = true)

  it should "handle unsuccessful create action correctly, check reasonCode adjustment, reasonForFailure (crudcontroller)" in {

    // adjustment: severity should be adjusted from normal to warning
    // test reasonForFailure with a response containing an error entitiy

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "PUT"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUpdateInfo, ""),
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/hello123?overwrite=false"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_create_action_err")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null

    val resp = HttpResponse(
      status = StatusCodes.Conflict,
      entity = HttpEntity(ContentTypes.`application/json`, """{"id": "id", "error": "error message"}"""))

    Await.result(activityTracker.responseHandlerAsync(transid, resp), waitTime)

    // in case of errors following additional settings hold:
    // - reason.reasonForFailure must be filled
    // - message must contain "-failure"
    // - outcome is "failure"
    val expectedString =
      """
{"requestData":{
    "actionName":"hello123",
    "requestId":"test_create_action_err",
    "url":"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/hello123?overwrite=false",
    "method":"PUT"
 },
 "resourceGroupId":"crn:v1:bluemix:public:resource-controller:global:a/eb2e36585c91a27a709c44e2652a381a::resource-group:ca23a1a3f0a84e2ab6b70c22ec6b1324",
 "observer":{"name":"ActivityTracker"},
 "outcome":"failure",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":409,
     "reasonType":"Conflict",
     "reasonForFailure":"error message"
 },
 "eventTime":"2020-06-02T18:42:55.149+0000",
 "message":"Functions: create action hello123 for namespace a88c0a24-853b-4477-82f8-6876e72bebf2 -failure",
 "target":{
     "id":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::",
     "name":"",
     "typeURI":"functions/namespace/action"
 },
 "severity":"warning",
 "logSourceCRN":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
 "action":"functions.action.create",
 "initiator":{
     "name":"john.doe@acme.com",
     "host":{
         "address":"192.168.0.1",
         "addressType":"IPv4",
         "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
      },
      "id":"IBMid-310000GN7M",
      "typeURI":"service/security/account/user",
      "credential":{
          "type":"apikey"
      }
 },
 "dataEvent":true,
 "responseData":{}
}
"""
    verifyEvent(eventString, expectedString)
  }

  it should "handle successful create action in classic namespace correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    // BasicAuth is used
    val settings = Seq(
      (TransactionId.tagGrantType, "password"),
      (TransactionId.tagHttpMethod, "PUT"),
      (TransactionId.tagInitiatorId, "john.doe@acme.com"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, ""), // not set for BasicAuth
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, ""), // not filles for classic namespaces
      (TransactionId.tagTargetId, ""), // only filled for IAM Auth (in this case tagTargetIdEncoded is empty)
      (
        TransactionId.tagTargetIdEncoded,
        "Y3JuOnYxOmJsdWVtaXg6cHVibGljOmZ1bmN0aW9uczp1cy1zb3V0aDphL2ViMmUzNjU4NWM5MWEyN2E3MDljNDRlMjY1MmEzODFhOnMtM2M5ZjJlZDgtNjQzNi00Mjg4LTkzYWQtMTgxNWI3ZWExMGE2Ojo="),
      (TransactionId.tagUpdateInfo, ""),
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/helloClassic1?overwrite=false"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_create_action_classic")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    val expectedString =
      """
{"requestData":{
    "actionName":"helloClassic1",
    "requestId":"test_create_action_classic",
    "method":"PUT",
    "url":"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/helloClassic1?overwrite=false"
 },
 "resourceGroupId":"",
 "observer":{
     "name":"ActivityTracker"
 },
 "outcome":"success",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":200,
     "reasonType":"OK"
 },
 "eventTime":"2020-06-03T00:47:51.818+0000",
 "message":"Functions: create action helloClassic1 for namespace s-3c9f2ed8-6436-4288-93ad-1815b7ea10a6",
 "target":{
     "id":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:s-3c9f2ed8-6436-4288-93ad-1815b7ea10a6::",
     "name":"",
     "typeURI":"functions/namespace/action"
 },
 "severity":"normal",
 "logSourceCRN":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
 "action":"functions.action.create",
 "initiator":{
     "name":"john.doe@acme.com",
     "host":{
         "address":"192.168.0.1",
         "addressType":"IPv4",
         "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
     },
     "id":"john.doe@acme.com",
     "typeURI":"service/security/account/user",
     "credential":{
         "type":"user"
     }
 },
 "dataEvent":true,
 "responseData":{}
}
"""
    verifyEvent(eventString, expectedString)
  }

  it should "create no activity event for get all (actions, packages, rules, triggers) (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    for (entityType <- Seq("action", "package", "rule", "trigger")) {

      val settings = Seq(
        (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
        (TransactionId.tagHttpMethod, "GET"),
        (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
        (TransactionId.tagInitiatorIp, "192.168.0.1"),
        (TransactionId.tagInitiatorName, "john.doe@acme.com"),
        (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
        (TransactionId.tagRequestedStatus, ""), // only filled for rules
        (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
        (
          TransactionId.tagTargetId,
          "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
        (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
        (
          TransactionId.tagUri,
          s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/${entityType}s?limit=30&skip=0"),
        (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

      val transid = TransactionId("test_get_all_action")
      for (setting <- settings) transid.setTag(setting._1, setting._2)

      eventString = null
      Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

      eventString shouldBe null
    }
  }

  it should "create no activity event for get activation (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/activations/2755eba166ba4f4095eba166babf4096"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "create no activity event for get all namespaces (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces?limit=30&skip=0"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_get_all_namespaces")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "handle successful (create, get, delete, update) (action, package, rule, trigger) correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    // sequences indexed by methodIndex
    val method = Seq("PUT", "GET", "DELETE", "PUT")
    val operation = Seq("create", "read", "delete", "update")
    val severity = Seq("normal", "normal", "critical", "warning")
    val actionType = operation

    val reasonCode = 200 // // always 200
    val reasonType = getReasonType(reasonCode.toString)

    for (methodIndex <- 0 to 3) {

      for (entityType <- Seq("action", "package", "rule", "trigger")) {

        val entityName = "hello123"

        val url =
          s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/${entityType}s/$entityName?key=value"

        val settings = Seq(
          (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
          (TransactionId.tagHttpMethod, method(methodIndex)),
          (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
          (TransactionId.tagInitiatorIp, "192.168.0.1"),
          (TransactionId.tagInitiatorName, "john.doe@acme.com"),
          (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
          (TransactionId.tagRequestedStatus, ""), // only filled for rules
          (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
          (
            TransactionId.tagTargetId,
            "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
          (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
          (TransactionId.tagUpdateInfo, if (operation(methodIndex) == "update") "true" else ""),
          (TransactionId.tagUri, url),
          (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

        val transid = TransactionId("test_api")
        for (setting <- settings) transid.setTag(setting._1, setting._2)

        eventString = null
        Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

        val expectedString =
          s"""
{"requestData":{
    "requestId":"test_api",
    "${entityType}Name": "$entityName",
    "method":"${method(methodIndex)}",
    "url":"$url"
 },
 "resourceGroupId":"crn:v1:bluemix:public:resource-controller:global:a/eb2e36585c91a27a709c44e2652a381a::resource-group:ca23a1a3f0a84e2ab6b70c22ec6b1324",
 "observer":{
    "name":"ActivityTracker"
 },
 "outcome":"success",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":$reasonCode,
     "reasonType":"$reasonType"
 },
 "eventTime":"2020-06-03T14:38:10.258+0000",
 "message":"Functions: ${operation(methodIndex)} $entityType $entityName for namespace a88c0a24-853b-4477-82f8-6876e72bebf2",
 "target":{
     "id":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::",
     "name":"",
     "typeURI":"functions/namespace/$entityType"
 },
 "severity":"${severity(methodIndex)}",
 "logSourceCRN":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
 "action":"functions.$entityType.${actionType(methodIndex)}",
 "initiator":{
     "name":"john.doe@acme.com",
     "host":{
         "address":"192.168.0.1",
         "addressType":"IPv4",
         "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
     },
     "id":"IBMid-310000GN7M",
     "typeURI":"service/security/account/user",
     "credential":{
         "type":"apikey"
     }
 },
 "dataEvent":true,
 "responseData":{}
}
"""
        verifyEvent(eventString, expectedString)
      }
    }
  }

  it should "create no activity event for post method on (action, trigger) (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    for (entityType <- Seq("action", "trigger")) {

      val entityName = "hello123"

      val url =
        s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/${entityType}s/$entityName?key=value"

      val settings = Seq(
        (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
        (TransactionId.tagHttpMethod, "POST"),
        (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
        (TransactionId.tagInitiatorIp, "192.168.0.1"),
        (TransactionId.tagInitiatorName, "john.doe@acme.com"),
        (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
        (TransactionId.tagRequestedStatus, ""), // only filled for rules
        (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
        (
          TransactionId.tagTargetId,
          "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
        (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
        (TransactionId.tagUri, url),
        (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

      val transid = TransactionId("test_api")
      for (setting <- settings) transid.setTag(setting._1, setting._2)

      eventString = null
      Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

      eventString shouldBe null
    }
  }

  it should "handle invalid grantType correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    var settings = Seq(
      (TransactionId.tagGrantType, "INVALID"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/testrule"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    var transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    val expectedString = getExpectedResultForInvalidValuesTests(credType = "unknown")
    verifyEvent(eventString, expectedString)
  }

  it should "handle invalid httpMethod correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    var settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "INVALID"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/testrule"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    var transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "handle invalid targetId correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    var settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (TransactionId.tagTargetId, "INVALID"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/testrule"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    var transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    val expectedString =
      getExpectedResultForInvalidValuesTests(
        targetId = "INVALID",
        logSourceCRN = "INVALID",
        accountInResourceGroupId = "unknown")

    verifyEvent(eventString, expectedString)
  }

  it should "handle invalid targetIdEncoded correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    var settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (TransactionId.tagTargetId, ""),
      (TransactionId.tagTargetIdEncoded, "NOTBASE64"), // only filled for BasicAuth (in this case tagTargetId is empty)
      (
        TransactionId.tagUri,
        "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/testrule"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    var transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    // "NOTBASE64" cannot be decoded via base64 therefore both targetid and logSourceCRN are empty
    val expectedString =
      getExpectedResultForInvalidValuesTests(targetId = "", logSourceCRN = "", accountInResourceGroupId = "unknown")

    verifyEvent(eventString, expectedString)
  }

  it should "handle invalid urls correctly (crudcontroller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = true
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    var settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "GET"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUri, "THIS_IS_NO_URL!"),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    var transid = TransactionId("test_get_activation")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    eventString = null
    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "handle the isActive flag correctly (crudcontroller)" in {

    // This is the create action test case with isActive = false and true.
    // The result should be empty if isActive = false. Otherwise, the expected event should
    // be the related event for creating an action.

    var eventString: String = null

    for (activeFlag <- Seq(false, true)) {

      val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
        override def isActive = activeFlag
        override def getIsCrudController: Boolean = true
        override def store(line: String): Unit = {
          eventString = line
        }
      }

      for (methodIndex <- 0 to 0) {

        // sequences indexed by methodIndex
        val method = Seq("PUT")
        val operation = Seq("create")
        val actionType = Seq("create")
        val reasonCode = 200 // // always 200
        val reasonType = getReasonType(reasonCode.toString)

        for (entityType <- Seq("action")) {

          val entityName = "hello123"

          val url =
            s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/${entityType}s/$entityName?key=value"

          val settings = Seq(
            (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
            (TransactionId.tagHttpMethod, method(methodIndex)),
            (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
            (TransactionId.tagInitiatorIp, "192.168.0.1"),
            (TransactionId.tagInitiatorName, "john.doe@acme.com"),
            (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
            (TransactionId.tagRequestedStatus, ""), // only filled for rules
            (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
            (
              TransactionId.tagTargetId,
              "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
            (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
            (TransactionId.tagUpdateInfo, ""),
            (TransactionId.tagUri, url),
            (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

          val transid = TransactionId("test_api")
          for (setting <- settings) transid.setTag(setting._1, setting._2)

          val expectedString =
            s"""
{"requestData":{
    "${entityType}Name":"$entityName",
    "requestId":"test_api",
    "method":"${method(methodIndex)}",
    "url":"$url"
 },
 "resourceGroupId":"crn:v1:bluemix:public:resource-controller:global:a/eb2e36585c91a27a709c44e2652a381a::resource-group:ca23a1a3f0a84e2ab6b70c22ec6b1324",
 "observer":{
    "name":"ActivityTracker"
 },
 "outcome":"success",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":$reasonCode,
     "reasonType":"$reasonType"
 },
 "eventTime":"2020-06-03T14:38:10.258+0000",
 "message":"Functions: ${operation(methodIndex)} $entityType $entityName for namespace a88c0a24-853b-4477-82f8-6876e72bebf2",
 "target":{
     "id":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::",
     "name":"",
     "typeURI":"functions/namespace/$entityType"
 },
 "severity":"normal",
 "logSourceCRN":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
 "action":"functions.$entityType.${actionType(methodIndex)}",
 "initiator":{
     "name":"john.doe@acme.com",
     "host":{
         "address":"192.168.0.1",
         "addressType":"IPv4",
         "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
     },
     "id":"IBMid-310000GN7M",
     "typeURI":"service/security/account/user",
     "credential":{
         "type":"apikey"
     }
 },
 "dataEvent":true,
 "responseData":{}
}
"""
          eventString = null
          Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

          if (activeFlag) verifyEvent(eventString, expectedString) else eventString shouldBe null
        }
      }
    }
  }

  // controller tests (getIsCrudController = false)
  // only http POST is tested since all requests with PUT, GET, DELETE are directed to crudcontroller

  it should "handle successful (enable, disable) rule correctly, also check IPv6 address (controller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = false
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    for (operationIndex <- 0 to 1) {

      // sequences indexed by operationIndex
      val requestedStatus = Seq("active", "inactive")
      val actionType = Seq("enable", "disable")

      val reasonCode = 200 // // always 200
      val reasonType = getReasonType(reasonCode.toString)
      val entityName = "hello123"
      val method = "POST"

      val url =
        s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/rules/$entityName"

      val settings = Seq(
        (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
        (TransactionId.tagHttpMethod, method),
        (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
        (TransactionId.tagInitiatorIp, "2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
        (TransactionId.tagInitiatorName, "john.doe@acme.com"),
        (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
        (TransactionId.tagRequestedStatus, requestedStatus(operationIndex)), // only filled for rules
        (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
        (
          TransactionId.tagTargetId,
          "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
        (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
        (TransactionId.tagUri, url),
        (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

      val transid = TransactionId("test_api")
      for (setting <- settings) transid.setTag(setting._1, setting._2)

      eventString = null
      Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

      val expectedString =
        s"""
{"requestData":{
    "ruleName":"$entityName",
    "requestId":"test_api",
    "method":"$method",
    "url":"$url"
 },
 "resourceGroupId":"crn:v1:bluemix:public:resource-controller:global:a/eb2e36585c91a27a709c44e2652a381a::resource-group:ca23a1a3f0a84e2ab6b70c22ec6b1324",
 "observer":{
    "name":"ActivityTracker"
 },
 "outcome":"success",
 "saveServiceCopy":true,
 "reason":{
     "reasonCode":$reasonCode,
     "reasonType":"$reasonType"
 },
 "eventTime":"2020-06-03T14:38:10.258+0000",
 "message":"Functions: ${actionType(operationIndex)} rule $entityName for namespace a88c0a24-853b-4477-82f8-6876e72bebf2",
 "target":{
     "id":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::",
     "name":"",
     "typeURI":"functions/namespace/rule"
 },
 "severity":"warning",
 "logSourceCRN":"crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a:::",
 "action":"functions.rule.${actionType(operationIndex)}",
 "initiator":{
     "name":"john.doe@acme.com",
     "host":{
         "address":"2001:0db8:85a3:0000:0000:8a2e:0370:7334",
         "addressType":"IPv6",
         "agent":"CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
     },
     "id":"IBMid-310000GN7M",
     "typeURI":"service/security/account/user",
     "credential":{
         "type":"apikey"
     }
 },
 "dataEvent":true,
 "responseData":{}
}
"""
      verifyEvent(eventString, expectedString)
    }
  }

  it should "create no event for a successful action invocation (controller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = false
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val url =
      s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/actions/hello123"

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "POST"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUri, url),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_api")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "create an event for a failed action invocation (controller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = false
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val method = "POST"
    val reasonCode = 401
    val reasonType = getReasonType(reasonCode.toString)
    val entityName = "hello123"
    val entityType = "action"
    val entityTypePlural = entityType + "s"
    val namespace = "a88c0a24-853b-4477-82f8-6876e72bebf2"
    val account = "eb2e36585c91a27a709c44e2652a381a"
    val resourceGroup = "ca23a1a3f0a84e2ab6b70c22ec6b1324"
    val initiatorId = "IBMid-310000GN7M"
    val initiator = "john.doe@acme.com"
    val ipAdress = "192.168.0.1"
    val region = "us-south"
    val agent = "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
    val targetId = s"crn:v1:bluemix:public:functions:$region:a/$account::$namespace::"

    val url =
      s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/$entityTypePlural/$entityName"

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, method),
      (TransactionId.tagInitiatorId, initiatorId),
      (TransactionId.tagInitiatorIp, ipAdress),
      (TransactionId.tagInitiatorName, initiator),
      (TransactionId.tagNamespaceId, namespace),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, resourceGroup),
      (TransactionId.tagTargetId, targetId),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUri, url),
      (TransactionId.tagUserAgent, agent))

    val transid = TransactionId("test_api")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.Unauthorized)), waitTime)

    val expectedString = s"""
  {
  "requestData": {
    "method": "$method",
    "url": "$url",
    "actionName": "$entityName",
    "requestId": "test_api"
  },
  "observer": {
    "name": "ActivityTracker"
  },
  "resourceGroupId": "crn:v1:bluemix:public:resource-controller:global:a/$account::resource-group:$resourceGroup",
  "outcome": "failure",
  "saveServiceCopy": true,
  "reason": {
    "reasonCode": $reasonCode,
    "reasonType": "$reasonType",
    "reasonForFailure": "$reasonType"
  },
  "eventTime": "2021-02-22T01:17:18.85+0000",
  "message": "Functions: activate $entityType $entityName for namespace $namespace -failure",
  "target": {
    "id": "$targetId",
    "name": "",
    "typeURI": "functions/namespace/$entityType"
  },
  "severity": "critical",
  "logSourceCRN": "crn:v1:bluemix:public:functions:$region:a/$account:::",
  "action": "functions.$entityType.activate",
  "initiator": {
    "name": "$initiator",
    "host": {
      "address": "$ipAdress",
      "addressType": "IPv4",
      "agent": "$agent"
    },
    "id": "$initiatorId",
    "typeURI": "service/security/account/user",
    "credential": {
      "type": "apikey"
    }
  },
  "dataEvent": true,
  "responseData": {}
}
"""

    verifyEvent(eventString, expectedString)
  }

  it should "create no event for firing a trigger in case of success (controller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = false
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val url =
      s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/triggers/hello123"

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, "POST"),
      (TransactionId.tagInitiatorId, "IBMid-310000GN7M"),
      (TransactionId.tagInitiatorIp, "192.168.0.1"),
      (TransactionId.tagInitiatorName, "john.doe@acme.com"),
      (TransactionId.tagNamespaceId, "a88c0a24-853b-4477-82f8-6876e72bebf2"),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, "ca23a1a3f0a84e2ab6b70c22ec6b1324"),
      (
        TransactionId.tagTargetId,
        "crn:v1:bluemix:public:functions:us-south:a/eb2e36585c91a27a709c44e2652a381a::a88c0a24-853b-4477-82f8-6876e72bebf2::"),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUri, url),
      (TransactionId.tagUserAgent, "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"))

    val transid = TransactionId("test_api")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.OK)), waitTime)

    eventString shouldBe null
  }

  it should "create an event for firing a trigger in case of a failure (controller)" in {

    var eventString: String = null

    val activityTracker = new ActivityTracker(actorSystem, materializer, logging) {
      override def isActive = true
      override def getIsCrudController: Boolean = false
      override def store(line: String): Unit = {
        eventString = line
      }
    }

    val method = "POST"
    val reasonCode = 401
    val reasonType = getReasonType(reasonCode.toString)
    val entityName = "hello123"
    val entityType = "trigger"
    val entityTapePlural = entityType + "s"
    val namespace = "a88c0a24-853b-4477-82f8-6876e72bebf2"
    val account = "eb2e36585c91a27a709c44e2652a381a"
    val resourceGroup = "ca23a1a3f0a84e2ab6b70c22ec6b1324"
    val initiatorId = "IBMid-310000GN7M"
    val initiator = "john.doe@acme.com"
    val ipAdress = "192.168.0.1"
    val region = "us-south"
    val agent = "CloudFunctions-Plugin/1.0 (2020-03-27T16:04:13+00:00) darwin amd64"
    val targetId = s"crn:v1:bluemix:public:functions:$region:a/$account::$namespace::"

    val url =
      s"https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/$entityTapePlural/$entityName"

    val settings = Seq(
      (TransactionId.tagGrantType, "urn:ibm:params:oauth:grant-type:apikey"),
      (TransactionId.tagHttpMethod, method),
      (TransactionId.tagInitiatorId, initiatorId),
      (TransactionId.tagInitiatorIp, ipAdress),
      (TransactionId.tagInitiatorName, initiator),
      (TransactionId.tagNamespaceId, namespace),
      (TransactionId.tagRequestedStatus, ""), // only filled for rules
      (TransactionId.tagResourceGroupId, resourceGroup),
      (TransactionId.tagTargetId, targetId),
      (TransactionId.tagTargetIdEncoded, ""), // only filled for BasicAuth (in this case tagTargetId is empty)
      (TransactionId.tagUri, url),
      (TransactionId.tagUserAgent, agent))

    val transid = TransactionId("test_api")
    for (setting <- settings) transid.setTag(setting._1, setting._2)

    Await.result(activityTracker.responseHandlerAsync(transid, HttpResponse(StatusCodes.Unauthorized)), waitTime)

    val expectedString = s"""
{
  "requestData": {
    "requestId": "test_api",
    "method": "$method",
    "url": "https://fn-dev-pg4.us-south.containers.appdomain.cloud/api/v1/namespaces/_/triggers/hello123",
    "triggerName": "hello123"
  },
  "observer": {
    "name": "ActivityTracker"
  },
  "resourceGroupId": "crn:v1:bluemix:public:resource-controller:global:a/$account::resource-group:$resourceGroup",
  "outcome": "failure",
  "saveServiceCopy": true,
  "reason": {
    "reasonCode": $reasonCode,
    "reasonType": "$reasonType",
    "reasonForFailure": "$reasonType"
  },
  "eventTime": "2021-02-22T17:37:50.05+0000",
  "message": "Functions: activate $entityType $entityName for namespace $namespace -failure",
  "target": {
    "id": "$targetId",
    "name": "",
    "typeURI": "functions/namespace/$entityType"
  },
  "severity": "critical",
  "logSourceCRN": "crn:v1:bluemix:public:functions:us-south:a/$account:::",
  "action": "functions.$entityType.activate",
  "initiator": {
    "name": "$initiator",
    "host": {
      "address": "$ipAdress",
      "addressType": "IPv4",
      "agent": "$agent"
    },
    "id": "$initiatorId",
    "typeURI": "service/security/account/user",
    "credential": {
      "type": "apikey"
    }
  },
  "dataEvent": true,
  "responseData": {}
}
"""

    verifyEvent(eventString, expectedString)
  }

}
