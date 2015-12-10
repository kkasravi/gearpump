package io.gearpump.experiments.yarn.appmaster

import java.io.File

import com.typesafe.config.Config
import io.gearpump.cluster.main.{Master, Worker}
import io.gearpump.experiments.yarn.Constants._
import io.gearpump.transport.HostPort
import io.gearpump.util.Constants
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

trait Command {
  def get: String
  override def toString: String = get
}

abstract class AbstractCommand extends Command {
  protected def config: Config
  def version: String
  def classPath = Array(
    s"conf",
    s"pack/$version/conf",
    s"pack/$version/lib/daemon/*",
    s"pack/$version/lib/*"
  )

  protected def buildCommand(java: String, properties: Array[String], mainClazz: String, cliOpts: Array[String]):String = {
    val exe = config.getString(java)

    s"$exe -cp ${classPath.mkString(File.pathSeparator)}${File.pathSeparator}" +
      "$CLASSPATH " + properties.mkString(" ") +
      s"  $mainClazz ${cliOpts.mkString(" ")} 2>&1 | /usr/bin/tee -a ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr"
  }

  protected def clazz(any: AnyRef) : String = {
    val name = any.getClass.getName
    if (name.endsWith("$")) {
      name.dropRight(1)
    } else {
      name
    }
  }
}

case class MasterCommand(config: Config, version: String, masterAddr: HostPort) extends AbstractCommand {

  def get: String = {
    val masterArguments = Array(s"-ip ${masterAddr.host}",  s"-port ${masterAddr.port}")

    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=${masterAddr.host}",
      s"-D${Constants.GEARPUMP_HOME}=${Environment.LOCAL_DIRS.$$()}/${Environment.CONTAINER_ID.$$()}/pack/$version",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}")

    buildCommand(MASTER_COMMAND, properties, clazz(Master), masterArguments)
  }
}

case class WorkerCommand(config: Config, version: String, masterAddr: HostPort, workerHost: String) extends AbstractCommand {

  def get: String = {
    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOME}=${Environment.LOCAL_DIRS.$$()}/${Environment.CONTAINER_ID.$$()}/pack/$version",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$workerHost")

    buildCommand(WORKER_COMMAND, properties,  clazz(Worker), Array.empty[String])
  }
}

case class AppMasterCommand(config: Config, version: String, args: Array[String]) extends AbstractCommand {

  override val classPath = Array(
    "conf",
    s"pack/$version/conf",
    s"pack/$version/dashboard",
    s"pack/$version/lib/*",
    s"pack/$version/lib/daemon/*",
    s"pack/$version/lib/services/*",
    s"pack/$version/lib/yarn/*"
  )

  def get: String = {
    val properties = Array(s"-D${Constants.GEARPUMP_HOME}=${Environment.LOCAL_DIRS.$$()}/${Environment.CONTAINER_ID.$$()}/pack/$version",
      s"-D${Constants.GEARPUMP_FULL_SCALA_VERSION}=$version")

    val arguments = Array(s"") ++ args

    buildCommand(APPMASTER_COMMAND, properties, clazz(YarnAppMaster),
      arguments)
  }
}