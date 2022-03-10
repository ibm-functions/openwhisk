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

import org.apache.openwhisk.common.TransactionId
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.apache.openwhisk.core.database.test.behavior.ArtifactStoreBehavior
import org.apache.openwhisk.core.entity.WhiskAuth

@RunWith(classOf[JUnitRunner])
class CouchDBArtifactStoreTests extends FlatSpec with CouchDBStoreBehaviorBase with ArtifactStoreBehavior {

  behavior of s"${storeType}ArtifactStore put"

  it should "not throw DocumentConflictException when updated with old revision" in {
    implicit val tid: TransactionId = transid()
    val auth = newAuth()
    val doc = put(authStore, auth)

    val auth2 = getWhiskAuth(doc).copy(namespaces = Set(wskNS("foo1"))).revision[WhiskAuth](doc.rev)
    val doc2 = put(authStore, auth2)

    //Updated with _rev set to older one
    val auth3 = getWhiskAuth(doc2).copy(namespaces = Set(wskNS("foo2"))).revision[WhiskAuth](doc.rev)
    val doc3 = put(authStore, auth3)
    doc3.rev should not be doc.rev
    doc3.rev should not be doc2.rev
    doc3.rev.empty shouldBe false
  }

  behavior of s"${storeType}ArtifactStore delete"

  it should "not throw DocumentConflictException when revision does not match" in {
    implicit val tid: TransactionId = transid()
    val auth = newAuth()
    val doc = put(authStore, auth)

    val auth2 = getWhiskAuth(doc).copy(namespaces = Set(wskNS("foo1"))).revision[WhiskAuth](doc.rev)
    val doc2 = put(authStore, auth2)

    delete(authStore, doc) shouldBe true
  }
}
