package io.gearpump.experiments.yarn.glue

import java.nio.ByteBuffer

import akka.actor.ActorRef
import com.typesafe.config.Config
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.ContainerStarted
import io.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerStatus}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl

/**
 * Adapter for node manager client
 */
class NMClient(yarnConf: YarnConfig, config: Config) extends NMClientAsync.CallbackHandler {

  private val LOG = LogUtil.getLogger(getClass)

  private var reportTo: ActorRef = null
  private var client: NMClientAsyncImpl = null

  def start(reportTo: ActorRef): Unit = {
    this.reportTo = reportTo
    client = new NMClientAsyncImpl(this)
    client.init(yarnConf.conf)
    client.start()
  }

  def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) {
    LOG.info(s"Container started : $containerId, " + allServiceResponse)
    reportTo ! ContainerStarted(containerId)
  }

  def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
    LOG.info(s"Container status received : $containerId, status $containerStatus")
  }

  def onContainerStopped(containerId: ContainerId) {
    LOG.error(s"Container stopped : $containerId")
  }

  def onGetContainerStatusError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStartContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def onStopContainerError(containerId: ContainerId, throwable: Throwable) {
    LOG.error(s"Container exception : $containerId", throwable)
  }

  def launchCommand(container: Container, command: String, packagePath: String, configPath: String): Unit = {
    LOG.info(s"Launching command : $command on container:  ${container.getId}, host ip : ${container.getNodeId.getHost}")
    val context = ContainerLaunchContext(yarnConf.conf, config, command, packagePath, configPath)
    client.startContainerAsync(container, context)
  }

  def stop(): Unit = {
    LOG.info(s"Shutdown NMClient")
    client.stop()
  }
}