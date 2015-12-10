package io.gearpump.experiments.yarn.glue

import com.typesafe.config.Config
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, ApplicationSubmissionContext, Resource, YarnApplicationState}
import org.apache.hadoop.yarn.client.api
import org.apache.hadoop.yarn.util.Records

/**
 * Adapter for api.YarnClient
 */
class YarnClient(yarn: YarnConfig, config: Config) {
  private val client: api.YarnClient = api.YarnClient.createYarnClient
  client.init(yarn.conf)
  client.start()

  def createApplication: ApplicationId = {
    val app  = client.createApplication()
    val response = app.getNewApplicationResponse()
    response.getApplicationId()
  }

  def submit(name: String, appId: ApplicationId, command: String, resource: Resource, queue: String, packagePath: String, configPath: String): ApplicationId = {

    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationName(name)
    appContext.setApplicationId(appId)

    val containerContext = ContainerLaunchContext(yarn.conf, config, command, packagePath, configPath)
    appContext.setAMContainerSpec(containerContext)
    appContext.setResource(resource)
    appContext.setQueue(queue)
    client.submitApplication(appContext)
  }

  def awaitApplicaition(appId: ApplicationId): ApplicationReport = {
    import YarnApplicationState._
    val terminated = Set(FINISHED, KILLED, FAILED, RUNNING)
    var result: ApplicationReport = null
    var done = false
    while(!done) {
      val report = client.getApplicationReport(appId)
      var status = report.getYarnApplicationState
      if (terminated.contains(status)) {
        done = true
        result = report
      } else {
        Console.print(".")
        Thread.sleep(1000)
      }
    }
    result
  }
}