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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.stream.scaladsl._
import akka.util.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.http.PoolingRestClient
import org.apache.openwhisk.http.PoolingRestClient._

import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.{ExecutionContext, Future}

/**
 * A client implementing the CouchDb API.
 *
 * This client only handles communication to the respective endpoints and works in a Json-in -> Json-out fashion. It's
 * up to the client to interpret the results accordingly.
 */
class CouchDbRestClient(protocol: String, host: String, port: Int, username: String, password: String, db: String)(
  implicit system: ActorSystem,
  logging: Logging)
    extends PoolingRestClient(protocol, host, port, 16 * 1024) {

  protected implicit override val context: ExecutionContext = system.dispatchers.lookup("dispatchers.couch-dispatcher")

  // Headers common to all requests.
  protected val baseHeaders: List[HttpHeader] =
    List(Authorization(BasicHttpCredentials(username, password)), Accept(MediaTypes.`application/json`))

  private def revHeader(forRev: String) = List(`If-Match`(EntityTagRange(EntityTag(forRev))))

  private val flexDb = db.endsWith("activations-")
  // activations database flex logic (consider two activations dbs for crud and view operations)
  case class ActivationsDbConfig(useFlexLogic: Boolean)
  private val activationsDbConfig =
    if (flexDb) loadConfig[ActivationsDbConfig]("whisk.activationsdb").toOption else None
  private val useFlexLogic = activationsDbConfig.exists(_.useFlexLogic)
  if (db.contains("activations")) logging.info(this, s"useFlexActivationsLogic: $useFlexLogic")
  def useFlexActivationsLogic: Boolean = useFlexLogic // use extended flexible activations database logic

  private val epochDay: Long = 24 * 60 * 60 * 1000
  private def getDbSfx: Long = System.currentTimeMillis / epochDay // get database suffix based on epoch day

  private def getDb: String = if (flexDb) db + getDbSfx else db // get fully qualified database name
  private def getDb(dbSfx: Long): String = db + dbSfx // get fully qualified database name using passed suffix

  // Properly encodes the potential slashes in each segment.
  protected def uri(segments: Any*): Uri = {
    val encodedSegments = segments.map(s => URLEncoder.encode(s.toString, StandardCharsets.UTF_8.name))
    Uri(s"/${encodedSegments.mkString("/")}")
  }

  // http://docs.couchdb.org/en/1.6.1/api/document/common.html#put--db-docid
  def putDoc(id: String, doc: JsObject): Future[Either[StatusCode, JsObject]] =
    requestJson[JsObject](mkJsonRequest(HttpMethods.PUT, uri(getDb, id), doc, baseHeaders))

  // http://docs.couchdb.org/en/1.6.1/api/document/common.html#put--db-docid
  def putDoc(id: String, rev: String, doc: JsObject): Future[Either[StatusCode, JsObject]] = {
    val dbSfx = if (useFlexLogic) getDbSfx else 0L // pin database suffix if needed
    requestJson[JsObject](
      mkJsonRequest(
        HttpMethods.PUT,
        uri(if (useFlexLogic) getDb(dbSfx) else getDb, id),
        doc,
        baseHeaders ++ revHeader(rev)))
      .flatMap { e =>
        e match {
          case Left(StatusCodes.NotFound) if useFlexLogic =>
            requestJson[JsObject](
              mkJsonRequest(HttpMethods.PUT, uri(getDb(dbSfx - 1), id), doc, baseHeaders ++ revHeader(rev)))
          case _ => Future(e)
        }
      }
  }

  // http://docs.couchdb.org/en/2.1.0/api/database/bulk-api.html#inserting-documents-in-bulk
  def putDocs(docs: Seq[JsObject]): Future[Either[StatusCode, JsArray]] =
    requestJson[JsArray](
      mkJsonRequest(HttpMethods.POST, uri(getDb, "_bulk_docs"), JsObject("docs" -> docs.toJson), baseHeaders))

  // http://docs.couchdb.org/en/1.6.1/api/document/common.html#get--db-docid
  def getDoc(id: String): Future[Either[StatusCode, JsObject]] = {
    val dbSfx = if (useFlexLogic) getDbSfx else 0L
    requestJson[JsObject](
      mkRequest(HttpMethods.GET, uri(if (useFlexLogic) getDb(dbSfx) else getDb, id), headers = baseHeaders))
      .flatMap { e =>
        e match {
          case Left(StatusCodes.NotFound) if useFlexLogic =>
            requestJson[JsObject](mkRequest(HttpMethods.GET, uri(getDb(dbSfx - 1), id), headers = baseHeaders))
          case _ => Future(e)
        }
      }
  }

  // http://docs.couchdb.org/en/1.6.1/api/document/common.html#get--db-docid
  def getDoc(id: String, rev: String): Future[Either[StatusCode, JsObject]] = {
    val dbSfx = if (useFlexLogic) getDbSfx else 0L // pin database suffix if needed
    requestJson[JsObject](
      mkRequest(
        HttpMethods.GET,
        uri(if (useFlexLogic) getDb(dbSfx) else getDb, id),
        headers = baseHeaders ++ revHeader(rev)))
      .flatMap { e =>
        e match {
          case Left(StatusCodes.NotFound) if useFlexLogic =>
            requestJson[JsObject](
              mkRequest(HttpMethods.GET, uri(getDb(dbSfx - 1), id), headers = baseHeaders ++ revHeader(rev)))
          case _ => Future(e)
        }
      }
  }

  // http://docs.couchdb.org/en/1.6.1/api/document/common.html#delete--db-docid
  def deleteDoc(id: String, rev: String): Future[Either[StatusCode, JsObject]] =
    requestJson[JsObject](mkRequest(HttpMethods.DELETE, uri(getDb, id), headers = baseHeaders ++ revHeader(rev)))

  // http://docs.couchdb.org/en/1.6.1/api/ddoc/views.html
  def executeView(designDoc: String, viewName: String)(startKey: List[Any] = Nil,
                                                       endKey: List[Any] = Nil,
                                                       skip: Option[Int] = None,
                                                       limit: Option[Int] = None,
                                                       stale: StaleParameter = StaleParameter.No,
                                                       includeDocs: Boolean = false,
                                                       descending: Boolean = false,
                                                       reduce: Boolean = false,
                                                       group: Boolean = false): Future[Either[StatusCode, JsObject]] = {

    require(reduce || !group, "Parameter 'group=true' cannot be used together with the parameter 'reduce=false'.")

    def any2json(any: Any): JsValue = any match {
      case b: Boolean => JsBoolean(b)
      case i: Int     => JsNumber(i)
      case l: Long    => JsNumber(l)
      case d: Double  => JsNumber(d)
      case f: Float   => JsNumber(f)
      case s: String  => JsString(s)
      case _ =>
        logging.warn(
          this,
          s"Serializing uncontrolled type '${any.getClass}' to string in JSON conversion ('${any.toString}').")
        JsString(any.toString)
    }

    def list2OptJson(lst: List[Any]): Option[JsValue] = {
      lst match {
        case Nil => None
        case _   => Some(JsArray(lst.map(any2json): _*))
      }
    }

    def bool2OptStr(bool: Boolean): Option[String] = if (bool) Some("true") else None

    val countargs = Seq[(String, Option[String])](
      "startkey" -> list2OptJson(startKey).map(_.toString),
      "endkey" -> list2OptJson(endKey).map(_.toString),
      "stale" -> stale.value,
      "descending" -> bool2OptStr(descending),
      "reduce" -> Some(true.toString))

    val baseargs = Seq[(String, Option[String])](
      "startkey" -> list2OptJson(startKey).map(_.toString),
      "endkey" -> list2OptJson(endKey).map(_.toString),
      "stale" -> stale.value,
      "include_docs" -> bool2OptStr(includeDocs),
      "descending" -> bool2OptStr(descending),
      "reduce" -> Some(reduce.toString),
      "group" -> bool2OptStr(group))

    def args(skip: Option[Int], limit: Option[Int]): Seq[(String, Option[String])] = {
      baseargs ++
        (Seq[(String, Option[String])](
          "skip" -> skip.filter(_ > 0).map(_.toString),
          "limit" -> limit.filter(_ > 0).map(_.toString)))
    }

    // Throw out all undefined arguments.
    def argMap(args: Seq[(String, Option[String])]): Map[String, String] =
      args
        .collect({
          case (l, Some(r)) => (l, r)
        })
        .toMap

    if (!useFlexLogic || group || reduce) {
      if (flexDb)
        logging.warn(this, s"Parameter 'group=$group' or 'reduce=$reduce' set for activations database '${getDb}'.")
      val viewUri =
        uri(getDb, "_design", designDoc, "_view", viewName)
          .withQuery(Uri.Query(argMap(args(skip = skip, limit = limit))))
      requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders))
    } else {
      val dbSfx = getDbSfx // pin database suffix
      val sk = skip.getOrElse(0)
      val li = limit.getOrElse(0)

      val viewUri =
        uri(getDb(dbSfx), "_design", designDoc, "_view", viewName)
          .withQuery(Uri.Query(argMap(args(skip = skip, limit = limit))))
      requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders)).flatMap { e =>
        e match {
          case Right(response) =>
            val rows1 = response.fields("rows").convertTo[List[JsObject]]
            (sk match {
              case _ if sk > 0 && rows1.length == 0 =>
                // count (reduce=true) call to determine number of activations to mitigate fuzziness
                // empty row set is either returned because there are no matching rows or
                // its count is within the number to be excluded as specified by the skip param
                val viewUri =
                  uri(getDb(dbSfx), "_design", designDoc, "_view", viewName)
                    .withQuery(Uri.Query(argMap(countargs)))
                requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders))
              case _ =>
                Future(Right(JsObject("rows" -> JsArray.empty))) // empty dummy response
            }).flatMap { ecount =>
              ecount match {
                case Right(countresponse) =>
                  val li2 = if (li > 0) li - rows1.length else li // adjust limit
                  (li match {
                    case _ if (li == 0 || li2 > 0) =>
                      // do second query call only if unlimited or limit is not exhausted
                      val sk2 = // adjust skip
                        if (sk > 0 && rows1.length == 0) {
                          // check response from count call to mitigate fuzziness
                          val rows = countresponse.fields("rows").convertTo[List[JsObject]]
                          val count = if (rows.nonEmpty) rows.head.fields("value").convertTo[Long] else 0
                          if (sk > count) sk - count else 0
                        }.toInt
                        else 0
                      val viewUri =
                        uri(getDb(dbSfx - 1), "_design", designDoc, "_view", viewName)
                          .withQuery(Uri.Query(argMap(args(skip = Some(sk2), limit = Some(li2)))))
                      requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders))
                    case _ =>
                      Future(Right(JsObject("rows" -> JsArray.empty))) // empty dummy response
                  }).flatMap { e2 =>
                    e2 match {
                      case Right(response2) =>
                        val rows2 = response2.fields("rows").convertTo[List[JsObject]]
                        Future(
                          Right(JsObject("rows" -> (rows1 ++ rows2).toArray.toJson
                            .convertTo[JsArray])))
                      case _ => Future(e2) // return left response from second query call
                    }
                  }
                case _ => Future(ecount) // return left response from count call
              }
            }
          case _ => Future(e) // return left response from first query call
        }
      }
    }
  }

  // http://docs.couchdb.org/en/1.6.1/api/ddoc/views.html
  def executeViewForCount(designDoc: String, viewName: String)(
    startKey: List[Any] = Nil,
    endKey: List[Any] = Nil,
    stale: StaleParameter = StaleParameter.No): Future[Either[StatusCode, JsObject]] = {

    def any2json(any: Any): JsValue = any match {
      case b: Boolean => JsBoolean(b)
      case i: Int     => JsNumber(i)
      case l: Long    => JsNumber(l)
      case d: Double  => JsNumber(d)
      case f: Float   => JsNumber(f)
      case s: String  => JsString(s)
      case _ =>
        logging.warn(
          this,
          s"Serializing uncontrolled type '${any.getClass}' to string in JSON conversion ('${any.toString}').")
        JsString(any.toString)
    }

    def list2OptJson(lst: List[Any]): Option[JsValue] = {
      lst match {
        case Nil => None
        case _   => Some(JsArray(lst.map(any2json): _*))
      }
    }

    def bool2OptStr(bool: Boolean): Option[String] = if (bool) Some("true") else None

    val args = Seq[(String, Option[String])](
      "startkey" -> list2OptJson(startKey).map(_.toString),
      "endkey" -> list2OptJson(endKey).map(_.toString),
      "stale" -> stale.value,
      "reduce" -> Some(true.toString))

    // Throw out all undefined arguments.
    val argMap: Map[String, String] = args
      .collect({
        case (l, Some(r)) => (l, r)
      })
      .toMap

    if (!useFlexLogic) {
      val viewUri =
        uri(getDb, "_design", designDoc, "_view", viewName).withQuery(Uri.Query(argMap))
      requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders))
    } else {
      val dbSfx = getDbSfx // pin database suffix
      val viewUri =
        uri(getDb(dbSfx), "_design", designDoc, "_view", viewName).withQuery(Uri.Query(argMap))
      requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders)).flatMap { e =>
        e match {
          case Right(response) =>
            val rows = response.fields("rows").convertTo[List[JsObject]]
            rows match {
              case _ if rows.isEmpty || rows.length == 1 =>
                val viewUri =
                  uri(getDb(dbSfx - 1), "_design", designDoc, "_view", viewName).withQuery(Uri.Query(argMap))
                requestJson[JsObject](mkRequest(HttpMethods.GET, viewUri, headers = baseHeaders)).flatMap { e2 =>
                  e2 match {
                    case Right(response2) =>
                      val rows2 = response2.fields("rows").convertTo[List[JsObject]]
                      rows2 match {
                        case _ if rows2.isEmpty || rows2.length == 1 =>
                          // get count from first and second query response
                          val count1 = if (rows.nonEmpty) rows.head.fields("value").convertTo[Long] else 0L
                          val count2 = if (rows.nonEmpty) rows.head.fields("value").convertTo[Long] else 0L
                          // {"rows": [{"key": null, "value": 3136}]}
                          Future(
                            Right(JsObject(
                              "rows" -> JsArray(JsObject("key" -> JsNull, "value" -> JsNumber(count1 + count2))))))
                        case _ => Future(e2) // return right response from second call if assertion is violated
                      }
                    case _ => Future(e2) // return left response from second call
                  }
                }
              case _ => Future(e) // return right response from first call if assertion is violated
            }
          case _ => Future(e) // return left response from first call
        }
      }
    }
  }

  // https://docs.couchdb.org/en/1.6.1/api/database/changes.html
  def changes()(since: Option[String] = None,
                limit: Option[Int] = None,
                includeDocs: Boolean = false,
                descending: Boolean = false): Future[JsObject] = {

    def bool2OptStr(bool: Boolean): Option[String] = if (bool) Some("true") else None

    val args = Seq[(String, Option[String])](
      "since" -> since,
      "limit" -> limit.filter(_ > 0).map(_.toString),
      "include_docs" -> bool2OptStr(includeDocs),
      "descending" -> bool2OptStr(descending))

    // Throw out all undefined arguments.
    val argMap: Map[String, String] = args
      .collect({
        case (l, Some(r)) => (l, r)
      })
      .toMap

    val changesUri = uri(db, "_changes").withQuery(Uri.Query(argMap))

    logging.debug(this, s"doing _changes request on host $host with uri $changesUri")
    requestJson[JsObject](mkRequest(HttpMethods.GET, changesUri, headers = baseHeaders))
      .map {
        case Right(resp) =>
          resp
        case Left(code) =>
          throw new Exception("Unexpected http response code: " + code)
      }
  }

  // Streams an attachment to the database
  // http://docs.couchdb.org/en/1.6.1/api/document/attachments.html#put--db-docid-attname
  def putAttachment(id: String,
                    rev: String,
                    attName: String,
                    contentType: ContentType,
                    source: Source[ByteString, _]): Future[Either[StatusCode, JsObject]] = {
    val entity = HttpEntity.Chunked(contentType, source.map(bs => HttpEntity.ChunkStreamPart(bs)))
    val request =
      mkRequest(HttpMethods.PUT, uri(getDb, id, attName), Future.successful(entity), baseHeaders ++ revHeader(rev))
    requestJson[JsObject](request)
  }

  // Retrieves and streams an attachment into a Sink, producing a result of type T.
  // http://docs.couchdb.org/en/1.6.1/api/document/attachments.html#get--db-docid-attname
  def getAttachment[T](id: String,
                       rev: String,
                       attName: String,
                       sink: Sink[ByteString, Future[T]]): Future[Either[StatusCode, (ContentType, T)]] = {
    val httpRequest =
      mkRequest(HttpMethods.GET, uri(getDb, id, attName), headers = baseHeaders ++ revHeader(rev))

    request(httpRequest).flatMap { response =>
      if (response.status.isSuccess) {
        response.entity.withoutSizeLimit().dataBytes.runWith(sink).map(r => Right(response.entity.contentType, r))
      } else {
        response.discardEntityBytes().future.map(_ => Left(response.status))
      }
    }
  }
}
