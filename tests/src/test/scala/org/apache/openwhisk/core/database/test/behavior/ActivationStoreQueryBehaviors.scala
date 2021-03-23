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

import java.time.Instant

import scala.concurrent.duration.DurationInt
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.database.UserContext
import org.apache.openwhisk.core.entity.{EntityPath, WhiskActivation}
import spray.json.{JsNumber, JsObject}

import scala.util.Random

trait ActivationStoreQueryBehaviors extends ActivationStoreBehaviorBase {

  private val retriesOnTestFailures = 5
  private val waitBeforeRetry = 1.second

  protected def checkQueryActivations(namespace: String,
                                      name: Option[String] = None,
                                      skip: Int = 0,
                                      limit: Int = 1000,
                                      includeDocs: Boolean = false,
                                      since: Option[Instant] = None,
                                      upto: Option[Instant] = None,
                                      context: UserContext,
                                      expected: IndexedSeq[WhiskActivation])(implicit transid: TransactionId): Unit = {
    val result =
      name
        .map { n =>
          activationStore
            .listActivationsMatchingName(
              EntityPath(namespace),
              EntityPath(n),
              skip = skip,
              limit = limit,
              includeDocs = includeDocs,
              since = since,
              upto = upto,
              context = context)
        }
        .getOrElse {
          activationStore
            .listActivationsInNamespace(
              EntityPath(namespace),
              skip = skip,
              limit = limit,
              includeDocs = includeDocs,
              since = since,
              upto = upto,
              context = context)
        }
        .map { r =>
          r.fold(left => left, right => right.map(wa => if (includeDocs) wa.toExtendedJson() else wa.summaryAsJson))
        }
        .futureValue

    result should contain theSameElementsAs expected.map(wa =>
      if (includeDocs) wa.toExtendedJson() else wa.summaryAsJson)
  }

  protected def checkCountActivations(namespace: String,
                                      name: Option[EntityPath] = None,
                                      skip: Int = 0,
                                      since: Option[Instant] = None,
                                      upto: Option[Instant] = None,
                                      context: UserContext,
                                      expected: Long)(implicit transid: TransactionId): Unit = {
    val result = activationStore
      .countActivationsInNamespace(
        EntityPath(namespace),
        name = name,
        skip = skip,
        since = since,
        upto = upto,
        context = context)
      .futureValue

    result shouldBe JsObject(WhiskActivation.collectionName -> JsNumber(expected))
  }

  behavior of s"${storeType}ActivationStore listActivationsInNamespace"

  it should "find all entities" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"
          val action2 = s"action2_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          val activations2 = (1000 until 1100 by 10).map(newActivation(namespace, action2, _))
          activations2 foreach (store(_, context))

          checkQueryActivations(namespace, context = context, expected = activations ++ activations2)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should find all entities not successful, retrying.."))
  }

  it should "support since and upto filters" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(
            namespace,
            since = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.takeRight(4))

          checkQueryActivations(
            namespace,
            upto = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.take(7))

          checkQueryActivations(
            namespace,
            since = Some(Instant.ofEpochMilli(1030)),
            upto = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.take(7).takeRight(4))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should support since and upto filters not successful, retrying.."))
  }

  it should "support skipping results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(namespace, skip = 5, context = context, expected = activations.take(5))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should support skipping results not successful, retrying.."))
  }

  it should "support limiting results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(namespace, limit = 5, context = context, expected = activations.takeRight(5))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should support limiting results not successful, retrying.."))
  }

  it should "support including complete docs" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(namespace, includeDocs = true, context = context, expected = activations)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should support including complete docs not successful, retrying.."))
  }

  it should "throw exception for negative limits and skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()

          a[IllegalArgumentException] should be thrownBy activationStore
            .listActivationsInNamespace(EntityPath("test"), skip = -1, limit = 10, context = context)

          a[IllegalArgumentException] should be thrownBy activationStore
            .listActivationsInNamespace(EntityPath("test"), skip = 0, limit = -1, context = context)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsInNamespace should throw exception for negative limits and skip not successful, retrying.."))
  }

  behavior of s"${storeType}ActivationStore listActivationsMatchingName"

  it should "find all entities matching name" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"
          val action2 = s"action2_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          val activations2 = (1000 until 1100 by 10).map(newActivation(namespace, action2, _))
          activations2 foreach (store(_, context))

          checkQueryActivations(namespace, Some(action1), context = context, expected = activations)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should find all entities matching name not successful, retrying.."))
  }

  it should "support since and upto filters" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(
            namespace,
            Some(action1),
            since = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.takeRight(4))

          checkQueryActivations(
            namespace,
            Some(action1),
            upto = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.take(7))

          checkQueryActivations(
            namespace,
            Some(action1),
            since = Some(Instant.ofEpochMilli(1030)),
            upto = Some(Instant.ofEpochMilli(1060)),
            context = context,
            expected = activations.take(7).takeRight(4))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should support since and upto filters not successful, retrying.."))
  }

  it should "support skipping results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(namespace, Some(action1), skip = 5, context = context, expected = activations.take(5))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should support skipping results not successful, retrying.."))
  }

  it should "support limiting results" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(
            namespace,
            Some(action1),
            limit = 5,
            context = context,
            expected = activations.takeRight(5))
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should support limiting results not successful, retrying.."))
  }

  it should "support including complete docs" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkQueryActivations(namespace, Some(action1), includeDocs = true, context = context, expected = activations)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should support including complete docs not successful, retrying.."))
  }

  it should "throw exception for negative limits and skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()

          a[IllegalArgumentException] should be thrownBy activationStore.listActivationsMatchingName(
            EntityPath("test"),
            name = EntityPath("testact"),
            skip = -1,
            limit = 10,
            context = context)

          a[IllegalArgumentException] should be thrownBy activationStore.listActivationsMatchingName(
            EntityPath("test"),
            name = EntityPath("testact"),
            skip = 0,
            limit = -1,
            context = context)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore listActivationsMatchingName should throw exception for negative limits and skip not successful, retrying.."))
  }

  behavior of s"${storeType}ActivationStore countActivationsInNamespace"

  it should "should count all created activations" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkCountActivations(namespace, None, context = context, expected = 10)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore countActivationsInNamespace should should count all created activations not successful, retrying.."))
  }

  it should "count with option name" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"
          val action2 = s"action2_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          val activations2 = (1000 until 1100 by 10).map(newActivation(namespace, action2, _))
          activations2 foreach (store(_, context))

          checkCountActivations(namespace, Some(EntityPath(action1)), context = context, expected = 10)

          checkCountActivations(namespace, Some(EntityPath(action2)), context = context, expected = 10)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore countActivationsInNamespace should should count with option name not successful, retrying.."))
  }

  it should "count with since and upto" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkCountActivations(
            namespace,
            None,
            since = Some(Instant.ofEpochMilli(1060L)),
            context = context,
            expected = 4)

          checkCountActivations(
            namespace,
            None,
            upto = Some(Instant.ofEpochMilli(1060L)),
            context = context,
            expected = 7)

          checkCountActivations(
            namespace,
            None,
            since = Some(Instant.ofEpochMilli(1030L)),
            upto = Some(Instant.ofEpochMilli(1060L)),
            context = context,
            expected = 4)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore countActivationsInNamespace should should count with since and upto not successful, retrying.."))
  }

  it should "count with skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()
          val namespace = s"ns_${Random.alphanumeric.take(4).mkString}"
          val action1 = s"action1_${Random.alphanumeric.take(4).mkString}"

          val activations = (1000 until 1100 by 10).map(newActivation(namespace, action1, _))
          activations foreach (store(_, context))

          checkCountActivations(namespace, None, skip = 4, context = context, expected = 10 - 4)
          checkCountActivations(namespace, None, skip = 1000, context = context, expected = 0)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore countActivationsInNamespace should should count with skip not successful, retrying.."))
  }

  it should "throw exception for negative skip" in {
    org.apache.openwhisk.utils
      .retry(
        {
          afterEach()
          implicit val tid: TransactionId = transId()

          a[IllegalArgumentException] should be thrownBy activationStore
            .countActivationsInNamespace(namespace = EntityPath("test"), name = None, skip = -1, context = context)
        },
        retriesOnTestFailures,
        Some(waitBeforeRetry),
        Some(
          s"${this.getClass.getName} > ${storeType}ActivationStore countActivationsInNamespace should should throw exception for negative skip not successful, retrying.."))
  }
}
