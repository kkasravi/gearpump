package io.gearpump.experiments.yarn.glue

import akka.actor.ActorRef
import io.gearpump.experiments.yarn.appmaster.YarnAppMaster.{AppMasterRegistered, ContainersAllocated, ContainersCompleted, ResourceManagerException, ShutdownApplication}
import io.gearpump.util.LogUtil
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerStatus, FinalApplicationStatus, NodeReport, Priority, Resource}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.util.Records
import org.slf4j.Logger

import scala.collection.JavaConverters._

/**
 * Adapter for resource manager client
 */
class RMClient(yarnConf: YarnConfig) extends AMRMClientAsync.CallbackHandler {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private var reportTo: ActorRef = null
  private var client: AMRMClientAsync[ContainerRequest] = null

  def start(reportTo: ActorRef): Unit = {
    this.reportTo = reportTo
    client = startAMRMClient
  }

  private def startAMRMClient: AMRMClientAsync[ContainerRequest] = {
    val timeIntervalMs = 1000 //ms
    val amrmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](timeIntervalMs, this)
    amrmClient.init(yarnConf.conf)
    amrmClient.start()
    amrmClient
  }

  def getProgress: Float = 0.5F

  private var allocatedContainers = Set.empty[ContainerId]
  def onContainersAllocated(containers: java.util.List[Container]) {
    val newContainers = containers.asScala.toList.filterNot(container => allocatedContainers.contains(container.getId))
    allocatedContainers ++= newContainers.map(_.getId)
    LOG.info(s"New allocated ${newContainers.size} containers")
    reportTo ! ContainersAllocated(newContainers)
  }

  def onContainersCompleted(completedContainers: java.util.List[ContainerStatus]) {
    LOG.info(s"Got response from RM. Completed containers=${completedContainers.size()}")
    reportTo ! ContainersCompleted(completedContainers.asScala.toList)
  }

  def onError(ex: Throwable) {
    LOG.info("Error occurred")
    reportTo ! ResourceManagerException(ex)
  }

  def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = {
    LOG.info("onNodesUpdates")
  }

  def onShutdownRequest() {
    LOG.info("Shutdown requested")
    reportTo ! ShutdownApplication
  }

  def requestContainers(containers: List[Resource]): Unit = {
    containers.foreach(resource => {
      client.addContainerRequest(createContainerRequest(resource))
    })
  }

  private def createContainerRequest(capability: Resource): ContainerRequest = {
    LOG.info("creating ContainerRequest")
    val priorityRecord = Records.newRecord(classOf[Priority])
    priorityRecord.setPriority(0)
    val priority = Priority.newInstance(0)
    new ContainerRequest(capability, null, null, priority)
  }

  def registerAppMaster(host: String, port: Int, url: String): Unit = {
    LOG.info(s"Received RegisterAMMessage! $host:$port $url")
    val response = client.registerApplicationMaster(host, port, url)
    LOG.info("Received RegisterAppMasterResponse ")
    reportTo ! AppMasterRegistered
  }

  def shutdownApplication(): Unit = {
    LOG.info(s"Shutdown application")
    client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "success", null)
    client.stop()
  }

  def failApplication(ex: Throwable): Unit = {
    LOG.error(s"Application failed! ", ex)
    client.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, ex.getMessage, null)
    client.stop()
  }
}
