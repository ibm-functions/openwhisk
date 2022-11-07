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

package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets
import java.time.Instant

import akka.Done
import akka.actor.{ActorRefFactory, ActorSystem, CoordinatedShutdown, Props}
import akka.event.Logging.InfoLevel
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.connector.{AcknowledegmentMessage, _}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.database.{UserContext, _}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

object InvokerReactive extends InvokerProvider {

  /**
   * A method for sending Active Acknowledgements (aka "active ack") messages to the load balancer. These messages
   * are either completion messages for an activation to indicate a resource slot is free, or result-forwarding
   * messages for continuations (e.g., sequences and conductor actions).
   *
   * The activation result is always provided because some acknowledegment messages may not carry the result of
   * the activation and this is needed for sending user events.
   *
   * @param tid the transaction id for the activation
   * @param activationResult is the activation result
   * @param blockingInvoke is true iff the activation was a blocking request
   * @param controllerInstance the originating controller/loadbalancer id
   * @param user is the user for the namespace owning the activation
   * @param acknowledegment the acknowledgement message to send
   */
  trait ActiveAck {
    def apply(tid: TransactionId,
              activationResult: WhiskActivation,
              blockingInvoke: Boolean,
              controllerInstance: ControllerInstanceId,
              user: Identity,
              acknowledegment: AcknowledegmentMessage): Future[Any]
  }

  /**
   * Collect logs after the activation has finished.
   *
   * This method is called after an activation has finished. The logs gathered here are stored along the activation
   * record in the database.
   *
   * @param transid transaction the activation ran in
   * @param user the user who ran the activation
   * @param activation the activation record
   * @param container container used by the activation
   * @param action action that was activated
   * @return logs for the given activation
   */
  trait LogsCollector {
    def logsToBeCollected(action: ExecutableWhiskAction): Boolean = action.limits.logs.asMegaBytes != 0.MB

    def apply(transid: TransactionId,
              user: Identity,
              activation: WhiskActivation,
              container: Container,
              action: ExecutableWhiskAction): Future[ActivationLogs]
  }

  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerReactive(config, instance, producer, poolConfig, limitsConfig)

}

class InvokerReactive(
  config: WhiskConfig,
  instance: InvokerInstanceId,
  producer: MessageProducer,
  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
  limitsConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](ConfigKeys.concurrencyLimit))(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends InvokerCore {

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  /**
   * Factory used by the ContainerProxy to physically create a new container.
   *
   * Create and initialize the container factory before kicking off any other
   * task or actor because further operation does not make sense if something
   * goes wrong here. Initialization will throw an exception upon failure.
   */
  private val containerFactory =
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(
        actorSystem,
        logging,
        config,
        instance,
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")) ++ logsProvider.containerParameters)
  containerFactory.init()

  CoordinatedShutdown(actorSystem)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "cleanup runtime containers") { () =>
      containerFactory.cleanup()
      Future.successful(Done)
    }

  /** Initialize needed databases */
  private val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, materializer, logging)

  private val authStore = WhiskAuthStore.datastore()

  private val namespaceBlacklist = new NamespaceBlacklist(authStore)

  private val imageStoreConfig = loadConfigOrThrow[CouchDbConfig]("whisk.couchdb")
  // config for invoker image monitor
  case class ImageMonitorConfig(enabled: Boolean,
                                cluster: String,
                                staleTime: Int,
                                writeInterval: Int,
                                buildNo: String,
                                deployDate: String,
                                ddoc: String,
                                view: String,
                                reqSuccViewCalls: Int,
                                retryDuration: Int)
  private val imageMonitorConfigNamespace = "whisk.invoker.imagemonitor"
  val imageMonitorConfig = loadConfig[ImageMonitorConfig](imageMonitorConfigNamespace).toOption
  private val imageMonitorEnabled = imageMonitorConfig.exists(_.enabled)
  private val imageMonitorCluster = imageMonitorConfig.map(_.cluster).getOrElse("")
  private val imageMonitorStaleTime = imageMonitorConfig.map(_.staleTime).getOrElse(-1)
  private val imageMonitorWriteInterval = imageMonitorConfig.map(_.writeInterval).getOrElse(-1)
  private val imageMonitorBuildNo = imageMonitorConfig.map(_.buildNo).getOrElse("")
  private val imageMonitorDeployDate = imageMonitorConfig.map(_.deployDate).getOrElse("")
  private val imageMonitorDDoc = imageMonitorConfig.map(_.ddoc).getOrElse("")
  private val imageMonitorView = imageMonitorConfig.map(_.view).getOrElse("")
  private val imageMonitorReqSuccViewCalls = imageMonitorConfig.map(_.reqSuccViewCalls).getOrElse(-1)
  private val imageMonitorRetryDuration = imageMonitorConfig.map(_.retryDuration).getOrElse(-1)
  logging.warn(
    this,
    s"imageStoreConfig : $imageStoreConfig, " +
      s"imageMonitorEnabled: $imageMonitorEnabled, " +
      s"imageMonitorCluster: $imageMonitorCluster, " +
      s"imageMonitorStaleTime: $imageMonitorStaleTime (${imageMonitorStaleTime * 1.day}), " +
      s"imageMonitorWriteInterval: $imageMonitorWriteInterval, " +
      s"imageMonitorBuildNo: $imageMonitorBuildNo, " +
      s"imageMonitorDeployDate: $imageMonitorDeployDate, " +
      s"imageMonitorDDoc: $imageMonitorDDoc, " +
      s"imageMonitorView: $imageMonitorView, " +
      s"imageMonitorReqSuccViewCalls: $imageMonitorReqSuccViewCalls, " +
      s"imageMonitorRetryDuration: $imageMonitorRetryDuration (${imageMonitorRetryDuration * 1.minute}(${(imageMonitorRetryDuration * 1.minute).toMillis}))")

  private lazy val imageStore = new CouchDbRestClient(
    imageStoreConfig.protocol,
    imageStoreConfig.host,
    imageStoreConfig.port,
    imageStoreConfig.username,
    imageStoreConfig.password,
    imageStoreConfig.databaseFor[ImageMonitor])

  private lazy val imageMonitor =
    new ImageMonitor(
      cluster = imageMonitorCluster,
      invoker = instance.instance,
      ip = instance.uniqueName.getOrElse(""),
      pod = instance.displayedName.getOrElse(""),
      fqname = instance.toString,
      staleTime = imageMonitorStaleTime * 1.day,
      build = imageMonitorDeployDate,
      buildNo = imageMonitorBuildNo,
      imageStore = imageStore,
      ddoc = imageMonitorDDoc,
      view = imageMonitorView,
      reqSuccViewCalls = imageMonitorReqSuccViewCalls,
      retryDuration = imageMonitorRetryDuration * 1.minute)

  /** Ensure images preload was able to run by checking if invoker config has changed. */
  def ensureImagePreload = if (imageMonitorEnabled) imageMonitor.sync else Future((true))

  private val rootfs = "/"
  private val logsfs = "/logs"
  private val fspcentmax = 85
  private var rootfspcent = 0
  private var logsfspcent = 0

  private def epochMinute: Long = System.currentTimeMillis / (60 * 1000) // epoch minute

  private val poolState = new ContainerPoolState

  Scheduler.scheduleWaitAtMost(60 seconds) { () =>
    logging.debug(this, "running background job to update blacklist")

    val firstrun = rootfspcent == 0
    //val rootfspcentraw = (s"df $rootfs" #| s"grep $rootfs" #| "awk '{ print $5}'" !!)
    val rootfsraw = Try((s"df $rootfs" #| s"grep $rootfs" !!).trim.replaceAll(" +", " ")).getOrElse("??")
    val rootfspcentraw = Try(rootfsraw.split(" ")(4)).getOrElse("??")
    rootfspcent = Try(rootfspcentraw.substring(0, rootfspcentraw.indexOf("%")).toInt).getOrElse(-1)
    val logsfsraw = Try((s"df $logsfs" #| s"grep $logsfs" !!).trim.replaceAll(" +", " ")).getOrElse("??")
    val logsfspcentraw = Try(logsfsraw.split(" ")(4)).getOrElse("??")
    logsfspcent = Try(logsfspcentraw.substring(0, logsfspcentraw.indexOf("%")).toInt).getOrElse(-1)
    logging.warn(
      this,
      s"invoker fs space: " +
        s"'$rootfsraw ($rootfspcentraw($rootfspcent($fspcentmax)))', " +
        s"'$logsfsraw ($logsfspcentraw($logsfspcent($fspcentmax)))', " +
        s"invoker container pool: " +
        s"freePoolSize: ${poolState.free} containers, " +
        s"busyPoolSize: ${poolState.busy} containers, " +
        s"prewarmedPoolSize: ${poolState.prewarmed} containers, " +
        s"waiting messages: ${poolState.waiting}")

    // write images to store if monitor is enabled and ready every nth minute
    if (imageMonitorEnabled && imageMonitor.isReady && epochMinute % imageMonitorWriteInterval == 0) {
      imageMonitor.write
    }

    // run database query at the start of the schedule and every fifth minute
    if (epochMinute % 5 == 0 || firstrun) {
      namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
        case Success(set) => {
          logging.warn(this, s"updated blacklist to ${set.size} entries")
          if (set.contains(instance.displayedName.getOrElse(""))) {
            logging.warn(this, s"invoker ${instance.toString} is blacklisted")
          }
        }
        case Failure(t) => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
      }
    } else {
      Future(())
    }
  }

  /** Initialize message consumers */
  private val topic = s"invoker${instance.toInt}"
  private val maximumContainers = (poolConfig.userMemory / MemoryLimit.MIN_MEMORY).toInt
  private val msgProvider = SpiLoader.get[MessagingProvider]

  //number of peeked messages - increasing the concurrentPeekFactor improves concurrent usage, but adds risk for message loss in case of crash
  private val maxPeek =
    math.max(maximumContainers, (maximumContainers * limitsConfig.max * poolConfig.concurrentPeekFactor).toInt)

  private val consumer =
    msgProvider.getConsumer(config, topic, topic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("activation", logging, consumer, maxPeek, 1.second, processActivationMessage)
  })

  private val ack = {
    val sender = if (UserEvents.enabled) Some(new UserEventSender(producer)) else None
    new MessagingActiveAck(producer, instance, sender)
  }

  private val collectLogs = new LogStoreCollector(logsProvider)

  /** Stores an activation in the database. */
  private val store = (tid: TransactionId, activation: WhiskActivation, context: UserContext) => {
    implicit val transid: TransactionId = tid
    activationStore.storeAfterCheck(activation, context)(tid, notifier = None)
  }

  /** Creates a ContainerProxy Actor when being called. */
  private val childFactory = (f: ActorRefFactory) =>
    f.actorOf(
      ContainerProxy
        .props(containerFactory.createContainer, ack, store, collectLogs, instance, poolConfig))

  val prewarmingConfigs: List[PrewarmingConfig] = {
    ExecManifest.runtimesManifest.stemcells.flatMap {
      case (mf, cells) =>
        cells.map { cell =>
          PrewarmingConfig(cell.count, new CodeExecAsString(mf, "", None), cell.memory)
        }
    }.toList
  }

  // failed to initialize reactive invoker:
  // you cannot create an instance of [org.apache.openwhisk.core.containerpool.ContainerPool] explicitly using the constructor (new)
  // you have to use one of the 'actorOf' factory methods to create a new actor
  // you can't call any methods within the actor directly (as this would break actor encapsulation)
  // you can only send messages to it and receive messages from it
  private val pool =
    actorSystem.actorOf(ContainerPool.props(childFactory, poolConfig, activationFeed, prewarmingConfigs, poolState))

  /** Is called when an ActivationMessage is read from Kafka */
  def processActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        // The message has been parsed correctly, thus the following code needs to *always* produce at least an
        // active-ack.

        implicit val transid: TransactionId = msg.transid

        //set trace context to continue tracing
        WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

        if (!namespaceBlacklist.isBlacklisted(msg.user)) {
          val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
          val namespace = msg.action.path
          val name = msg.action.name
          val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
          val subject = msg.user.subject

          logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

          // caching is enabled since actions have revision id and an updated
          // action will not hit in the cache due to change in the revision id;
          // if the doc revision is missing, then bypass cache
          if (actionid.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionid.id}")

          WhiskAction
            .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty)
            .flatMap { action =>
              action.toExecutableWhiskAction match {
                case Some(executable) =>
                  if (imageMonitorEnabled && imageMonitor.isReady && executable.exec.pull) {
                    imageMonitor.add(executable.exec.image.resolveImageName(), msg.action.fullPath.toString)
                  }
                  pool ! Run(executable, msg)
                  Future.successful(())
                case None =>
                  logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
                  Future.failed(new IllegalStateException("non-executable action reached the invoker"))
              }
            }
            .recoverWith {
              case t =>
                // If the action cannot be found, the user has concurrently deleted it,
                // making this an application error. All other errors are considered system
                // errors and should cause the invoker to be considered unhealthy.
                val response = t match {
                  case _: NoDocumentException =>
                    ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
                  case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                    ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
                  case _ =>
                    ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
                }

                activationFeed ! MessageFeed.Processed

                val activation = generateFallbackActivation(msg, response)
                ack(
                  msg.transid,
                  activation,
                  msg.blocking,
                  msg.rootControllerIndex,
                  msg.user,
                  CombinedCompletionAndResultMessage(transid, activation, instance))

                store(msg.transid, activation, UserContext(msg.user))
                Future.successful(())
            }
        } else {
          // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
          // Due to the protective nature of the blacklist, a database entry is not written.
          activationFeed ! MessageFeed.Processed

          val activation =
            generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))
          ack(
            msg.transid,
            activation,
            false,
            msg.rootControllerIndex,
            msg.user,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked in invoker.")
          Future.successful(())
        }
      }
      .recoverWith {
        case t =>
          // Iff everything above failed, we have a terminal error at hand. Either the message failed
          // to deserialize, or something threw an error where it is not expected to throw.
          activationFeed ! MessageFeed.Processed
          logging.error(this, s"terminal failure while processing message: $t")
          Future.successful(())
      }
  }

  /**
   * Generates an activation with zero runtime. Usually used for error cases.
   *
   * Set the kind annotation to `Exec.UNKNOWN` since it is not known to the invoker because the action fetch failed.
   */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(Exec.UNKNOWN)) ++ causedBy ++ Parameters(
          WhiskActivation.transIdAnnotation,
          JsString(msg.transid.id))
      })
  }

  private val healthProducer = msgProvider.getProducer(config)
  Scheduler.scheduleWaitAtMost(1.seconds)(() => {
    // ping only if monitor is ready when enabled
    if (!imageMonitorEnabled || imageMonitor.isReady) {
      healthProducer
        .send(
          "health",
          PingMessage(
            instance = instance,
            isBlacklisted = namespaceBlacklist.isBlacklisted(instance.displayedName.getOrElse("")),
            hasDiskPressure = rootfspcent >= fspcentmax || logsfspcent >= fspcentmax,
            rootfspcent = rootfspcent,
            logsfspcent = logsfspcent,
            running = poolState.busy + poolState.waiting))
        .andThen {
          case Failure(t) => logging.error(this, s"failed to ping the controller: $t")
        }
    } else Future(())
  })
}
