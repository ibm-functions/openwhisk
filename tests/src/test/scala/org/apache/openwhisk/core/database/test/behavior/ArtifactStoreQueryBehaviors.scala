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

package org.apache.openwhisk.core.database.test.behavior

import scala.concurrent.duration._
import spray.json.{JsArray, JsNumber, JsObject, JsString}
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.entity.WhiskQueries.TOP
import org.apache.openwhisk.core.entity.{EntityPath, WhiskAction, WhiskActivation, WhiskEntity}

trait ArtifactStoreQueryBehaviors extends ArtifactStoreBehaviorBase {

  behavior of s"${storeType}ArtifactStore query"

  it should "find single entity" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val action = newAction(ns)
          val docInfo = put(entityStore, action)

          waitOnView(entityStore, ns.root, 1, WhiskAction.view)
          val result = query[WhiskEntity](
            entityStore,
            WhiskAction.view.name,
            List(ns.asString, 0),
            List(ns.asString, TOP, TOP),
            includeDocs = true)

          result should have length 1

          def js = result.head
          js.fields("id") shouldBe JsString(docInfo.id.id)
          js.fields("key") shouldBe JsArray(JsString(ns.asString), JsNumber(action.updated.toEpochMilli))
          js.fields.get("value") shouldBe defined
          js.fields.get("doc") shouldBe defined
          js.fields("value") shouldBe action.summaryAsJson
          dropRev(js.fields("doc").asJsObject) shouldBe action.toDocumentRecord
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should find single entity not successful, retrying.."))
  }

  it should "not have doc with includeDocs = false" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val action = newAction(ns)
          val docInfo = put(entityStore, action)

          waitOnView(entityStore, ns.root, 1, WhiskAction.view)
          val result =
            query[WhiskEntity](entityStore, WhiskAction.view.name, List(ns.asString, 0), List(ns.asString, TOP, TOP))

          result should have length 1

          def js = result.head
          js.fields("id") shouldBe JsString(docInfo.id.id)
          js.fields("key") shouldBe JsArray(JsString(ns.asString), JsNumber(action.updated.toEpochMilli))
          js.fields.get("value") shouldBe defined
          js.fields.get("doc") shouldBe None
          js.fields("value") shouldBe action.summaryAsJson
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should not have doc with includeDocs = false not successful, retrying.."))
  }

  it should "find all entities" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val entities = Seq(newAction(ns), newAction(ns))

          entities foreach {
            put(entityStore, _)
          }

          waitOnView(entityStore, ns.root, 2, WhiskAction.view)
          val result =
            query[WhiskEntity](entityStore, WhiskAction.view.name, List(ns.asString, 0), List(ns.asString, TOP, TOP))

          result should have length entities.length
          result.map(_.fields("value")) should contain theSameElementsAs entities.map(_.summaryAsJson)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should find all entities not successful, retrying.."))
  }

  it should "exclude deleted entities" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val entities = Seq(newAction(ns), newAction(ns), newAction(ns))
          val validEntities = entities.tail
          val infos = entities.map(put(entityStore, _))

          delete(entityStore, infos.head)

          waitOnView(entityStore, ns.root, 2, WhiskAction.view)
          val result =
            query[WhiskEntity](entityStore, WhiskAction.view.name, List(ns.asString, 0), List(ns.asString, TOP, TOP))

          result should have length validEntities.length
          result.map(_.fields("value")) should contain theSameElementsAs validEntities.map(_.summaryAsJson)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should exclude deleted entities not successful, retrying.."))
  }

  it should "return result in sorted order" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)

          val resultDescending = query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP))

          resultDescending should have length activations.length
          resultDescending.map(getJsField(_, "value", "start")) shouldBe activations
            .map(_.summaryAsJson.fields("start"))
            .reverse

          val resultAscending = query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            descending = false)

          resultAscending.map(getJsField(_, "value", "start")) shouldBe activations.map(_.summaryAsJson.fields("start"))
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should return result in sorted order not successful, retrying.."))
  }

  it should "support skipping results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)
          val result = query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            skip = 5,
            descending = false)

          result.map(getJsField(_, "value", "start")) shouldBe activations.map(_.summaryAsJson.fields("start")).drop(5)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should support skipping results not successful, retrying.."))
  }

  it should "support limiting results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)
          val result = query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            limit = 5,
            descending = false)

          result.map(getJsField(_, "value", "start")) shouldBe activations.map(_.summaryAsJson.fields("start")).take(5)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should support limiting results not successful, retrying.."))
  }

  it should "support including complete docs" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)
          val result = query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            includeDocs = true,
            descending = false)

          //Drop the _rev field as activations do not have that field
          result.map(js => JsObject(getJsObject(js, "doc").fields - "_rev")) shouldBe activations.map(
            _.toDocumentRecord)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should support including complete docs not successful, retrying.."))
  }

  it should "throw exception for negative limits and skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()
          a[IllegalArgumentException] should be thrownBy query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List("foo", 0),
            List("foo", TOP, TOP),
            limit = -1)

          a[IllegalArgumentException] should be thrownBy query[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List("foo", 0),
            List("foo", TOP, TOP),
            skip = -1)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore query should throw exception for negative limits and skip not successful, retrying.."))
  }

  behavior of s"${storeType}ArtifactStore count"

  it should "should match all created activations" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)
          val result = count[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP))

          result shouldBe 10
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore count should should match all created activations not successful, retrying.."))
  }

  it should "count with skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()

          val ns = newNS()
          val activations = (1000 until 1100 by 10).map(newActivation(ns.asString, "testact", _))
          activations foreach (put(activationStore, _))

          val entityPath = s"${ns.asString}/testact"
          waitOnView(activationStore, EntityPath(entityPath), activations.size, WhiskActivation.filtersView)
          val result = count[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            skip = 4)

          result shouldBe 10 - 4

          val result2 = count[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List(entityPath, 0),
            List(entityPath, TOP, TOP),
            skip = 1000)

          result2 shouldBe 0
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore count should count with skip not successful, retrying.."))
  }

  it should "throw exception for negative skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          implicit val tid: TransactionId = transid()
          a[IllegalArgumentException] should be thrownBy count[WhiskActivation](
            activationStore,
            WhiskActivation.filtersView.name,
            List("foo", 0),
            List("foo", TOP, TOP),
            skip = -1)
        },
        10,
        Some(1.second),
        Some(
          s"${this.getClass.getName} > ${storeType}ArtifactStore count should throw exception for negative skip not successful, retrying.."))
  }

  private def dropRev(js: JsObject): JsObject = {
    JsObject(js.fields - "_rev")
  }
}
