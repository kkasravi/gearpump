/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.yarn.appmaster

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.{ConfigValueFactory, Config}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.main.{CLIOption, ArgumentsParser}
import io.gearpump.experiments.yarn.Constants._
import io.gearpump.experiments.yarn.glue.{NMClient, RMClient, YarnConfig}
import io.gearpump.transport.HostPort
import io.gearpump.util.{Constants, Util, AkkaApp, LogUtil}
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerStatus, Resource}
import org.slf4j.Logger
import io.gearpump.experiments.yarn.Constants.CONFIG_PATH

class YarnAppMaster(akkaConf: Config, rmClient: RMClient, nmClient: NMClient, packagePath: String, hdfsConfDir: String)
    extends Actor {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val servicesEnabled = akkaConf.getString(SERVICES_ENABLED).toBoolean
  private val host = InetAddress.getLocalHost.getHostName

  val port = Util.findFreePort.get

  private val trackingURL = "http://" + host + ":" + port

  private val masterCount = akkaConf.getString(MASTER_CONTAINERS).toInt
  private val masterMemory = akkaConf.getString(ASTER_MEMORY).toInt
  private val masterVCores = akkaConf.getString(MASTER_VCORES).toInt

  private val workerCount = akkaConf.getString(WORKER_CONTAINERS).toInt
  private val workerMemory = akkaConf.getString(WORKER_MEMORY).toInt
  private val workerVCores = akkaConf.getString(WORKER_VCORES).toInt

  val rootPath = System.getProperty(Constants.GEARPUMP_FULL_SCALA_VERSION)

  rmClient.start(self)
  nmClient.start(self)

  def receive: Receive = null

  private def registerAppMaster: Unit = {
    val target = host + ":" + port
    val addr = NetUtils.createSocketAddr(target)
    rmClient.registerAppMaster(addr.getHostName, port, trackingURL)
  }

  registerAppMaster
  context.become(waitForAppMasterRegistered)

  import YarnAppMaster._

  def waitForAppMasterRegistered: Receive = {
    case AppMasterRegistered =>
      requestMasterContainers(masterCount)
      context.become(startingMasters(remain = masterCount, List.empty[HostPort]))
  }

  private def startingMasters(remain: Int, masters: List[HostPort]) : Receive = box {
    case ContainersAllocated(containers) =>
      val newMasters = containers.map(container => launchMaster(container))
      context.become(startingMasters(remain, newMasters ++ masters))
    case ContainerStarted(containerId) =>
      if (remain > 1) {
        context.become(startingMasters(remain - 1, masters))
      } else {
        requestWorkerContainers(workerCount)
        context.become(startingWorkers(workerCount, masters))
      }
  }

  private def box(receive: Receive): Receive = {
    service orElse receive orElse unHandled
  }

  private def startingWorkers(remain: Int, masters: List[HostPort]): Receive =
    box {
    case ContainersAllocated(containers) =>
      containers.foreach(container => launchWorker(container, masters))
      context.become(startingWorkers(remain, masters))
    case ContainerStarted(containerId) =>
      // the last one
      if (remain > 1) {
        context.become(startingWorkers(remain - 1, masters))
      } else {
        if (servicesEnabled) {
          context.actorOf(Props(new UIService(masters, host, port)))
        }
        context.become(service)
      }
  }

  private def service: Receive  = {
    case ContainersCompleted(containers) =>
      //TODO: we should recover the failed container from this...
      containers.foreach { status =>
        if (status.getExitStatus != 0) {
          LOG.error(s"Container ${status.getContainerId} failed with exit code ${status.getExitStatus}, msg: ${status.getDiagnostics}")
        } else {
          LOG.error(s"Container ${status.getContainerId} completed")
        }
      }
    case ShutdownApplication =>
      nmClient.stop()
      rmClient.shutdownApplication()
    case ResourceManagerException(ex) =>
      nmClient.stop()
      rmClient.failApplication(ex)
  }

  private def unHandled: Receive = {
    case other =>
      LOG.info(s"Received unknown message $other")
  }

  private def requestMasterContainers(masters: Int) = {
    val containers = (1 to masters).map(
      i => Resource.newInstance(masterMemory, masterVCores)
    ).toList
    rmClient.requestContainers(containers)
  }

  private def launchMaster(container: Container): HostPort = {
    val host = container.getNodeId.getHost

    val port = Util.findFreePort.get

    LOG.info("=============PORT" + port)
    val masterCommand = MasterCommand(akkaConf, rootPath, HostPort(host, port))
    nmClient.launchCommand(container, masterCommand.get, packagePath, hdfsConfDir)
    HostPort(host, port)
  }

  private def requestWorkerContainers(workers: Int): Unit = {
    val containers = (1 to workers).map(
      i => Resource.newInstance(workerMemory, workerVCores)
    ).toList

    rmClient.requestContainers(containers)
  }

  private def launchWorker(container: Container, masters: List[HostPort]): Unit = {
    val workerHost = container.getNodeId.getHost
    val workerCommand = WorkerCommand(akkaConf, rootPath, masters.head, workerHost)
    nmClient.launchCommand(container, workerCommand.get, packagePath, hdfsConfDir)
  }
}

object YarnAppMaster extends AkkaApp with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "conf" -> CLIOption[String]("<Gearpump configuration directory on HDFS>", required = true),
    "package" -> CLIOption[String]("<Gearpump package path on HDFS>", required = true)
  )

  override def akkaConfig: Config = {
    ClusterConfig.load.ui
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    implicit val timeout = Timeout(5, TimeUnit.SECONDS)
    implicit val system = ActorSystem("GearPumpAM", akkaConf)

    val yarnConf = new YarnConfig()

    val confDir = parse(args).getString("conf")
    val packagePath = parse(args).getString("package")

    LOG.info("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"))
    LOG.info("YARN Resource Manager: " + yarnConf.resourceManager)

    val rmClient = new RMClient(yarnConf)
    val nmClient = new NMClient(yarnConf, akkaConf)
    system.actorOf(Props(new YarnAppMaster(akkaConf, rmClient, nmClient, packagePath, confDir)))
    system.awaitTermination()
    LOG.info("Shutting down")
    system.shutdown()
  }

  case class ResourceManagerException(throwable: Throwable)
  case object ShutdownApplication
  case class ContainersRequest(containers: List[Resource])
  case class ContainersAllocated(containers: List[Container])
  case class ContainersCompleted(containers: List[ContainerStatus])
  case class ContainerStarted(containerId: ContainerId)
  case object AppMasterRegistered
}