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
package io.gearpump.experiments.yarn.client

import java.io.{PrintWriter, FileOutputStream, InputStreamReader, IOException, FileInputStream, FileNotFoundException, OutputStreamWriter}
import java.util.zip.ZipInputStream

import com.typesafe.config.{Config, ConfigValueFactory}
import io.gearpump.cluster.ClusterConfig
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.experiments.yarn
import io.gearpump.experiments.yarn.Constants
import io.gearpump.experiments.yarn.appmaster.AppMasterCommand
import io.gearpump.experiments.yarn.glue.{YarnClient, YarnConfig}
import io.gearpump.util.{AkkaApp, LogUtil, Util}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.slf4j.Logger
import java.io.File

import scala.util.{Failure, Success, Try}
import org.apache.hadoop.fs.Path

class Client(val appName: String, val akka: Config, val yarnConf: YarnConfig, yarnClient: YarnClient, fs: FileSystem, gearPackagePath: String) {
  import yarn.Constants._

  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val queue = akka.getString(APPMASTER_QUEUE)
  private val memory = akka.getString(APPMASTER_MEMORY).toInt
  private val vcore = akka.getString(APPMASTER_VCORES).toInt

  private val version = Util.version

  def submit(): ApplicationReport = {
    LOG.info("Starting AM")

    // first step, check the version, to make sure local version matches remote version
    val rootEntry = rootEntryPath(zip = gearPackagePath)

    if (!rootEntry.contains(version)) {
      throw new IOException(s"Check version failed! Local gearpump binary version $version doesn't match with remote path $gearPackagePath")
    }

    val resource = Resource.newInstance(memory, vcore)
    val appId = yarnClient.createApplication

    // will upload the configs to HDFS home directory of current user.
    LOG.info("Uploading configuration files to remote HDFS(under /user/<id>/.gearpumpapp_)...")
    val configPath = uploadConfigToHDFS(appId)

    val command = AppMasterCommand(akka, rootEntry, Array(s"-conf $configPath", s"-package $gearPackagePath"))

    yarnClient.submit(appName, appId, command.get, resource, queue, gearPackagePath, configPath)

    LOG.info("Waiting application to finish...")
    val report = yarnClient.awaitApplicaition(appId)
    LOG.info(s"Application $appId finished with state ${report.getYarnApplicationState} at ${report.getFinishTime}, info: ${report.getDiagnostics}")
    Console.println("================================================")
    Console.println("===Application Id: " + appId)
    report
  }

  private def uploadConfigToHDFS(appId: ApplicationId): String = {
    // will use personal home directory so that it will not conflict with other users
    val confDir = new Path(fs.getHomeDirectory(), s".gearpump_app${appId.getId}/conf/")

    // copy config from local to remote.
    val remoteConfFile = new Path(confDir, "gear.conf")
    var out = fs.create(remoteConfFile)
    var writer = new OutputStreamWriter(out)

    val filterJvmReservedKeys = ClusterConfig.filterOutJvmReservedKeys(akka)

    writer.write(filterJvmReservedKeys.root().render())
    writer.close()
    out.close()

    // save yarn-site.xml to remote
    val yarn_site_xml = new Path(confDir, "yarn-site.xml")
    out = fs.create(yarn_site_xml)
    writer = new OutputStreamWriter(out)
    yarnConf.conf.writeXml(writer)
    writer.close()
    out.close()

    // save log4j.properties to remote
    val log4j_properties = new Path(confDir, "log4j.properties")
    val log4j = LogUtil.loadConfiguration
    out = fs.create(log4j_properties)
    writer = new OutputStreamWriter(out)
    log4j.store(writer, "gearpump on yarn")
    writer.close()
    out.close()
    confDir.toString
  }

  def rootEntryPath(zip: String): String = {
    val stream = new ZipInputStream(fs.open(new Path(zip)))
    val entry = stream.getNextEntry()
    val name = entry.getName
    name.substring(0, entry.getName.indexOf("/"))
  }
}

object Client extends AkkaApp with ArgumentsParser {

  override protected def akkaConfig: Config = {
    ClusterConfig.load.default
  }

  override val options: Array[(String, CLIOption[Any])] = Array(
    "launch" -> CLIOption[String]("<Please specify the gearpump.zip package path on HDFS. If not specified, we will use default value /user/gearpump/gearpump.zip>", required = false),
    "name" ->CLIOption[String]("<Application name showed in YARN>", required = false, defaultValue = Some("Gearpump")),
    "verbose" -> CLIOption("<print verbose log on console>", required = false, defaultValue = Some(false)),
    "storeConfig" -> CLIOption("<local path where the master config should be stored>", required = false, defaultValue = None)
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val parsed = parse(args)

    val name = parsed.getString("name")
    if (parsed.getBoolean("verbose")) {
      LogUtil.verboseLogToConsole
    }

    val yarnConfig = new YarnConfig()

    val packagePath = if (parsed.exists("launch")) {
      parsed.getString("launch")
    } else {
      akkaConf.getString(Constants.PACKAGE_PATH)
    }

    val yarnClient = new YarnClient(yarnConfig, akkaConf)

    val fs = FileSystem.get(yarnConfig.conf)
    val client = new Client(name, akkaConf, yarnConfig, yarnClient, fs, packagePath)

    Try {
      val report = client.submit()

      if (parsed.exists("storeConfig")) {
        val storeFile = new File(parsed.getString("storeConfig"))
        val configUrl = s"${report.getOriginalTrackingUrl}/api/v1.0/master/config"
        storeConfig(configUrl, storeFile)
      }

    }.recover {
      case ex: FileNotFoundException =>
        Console.err.println (s"${
          ex.getMessage
        }\n" +
          s"try to check if your gearpump version is right " +
          s"or HDFS was correctly configured.")
      case ex => help;
        throw ex
    }
  }

  private def storeConfig(url: String, targetFile: File): Unit = {
    val client = new HttpClient()
    val get = new GetMethod(url)
    val status = client.executeMethod(get)

    val out = new PrintWriter(targetFile)
    out.print(get.getResponseBodyAsString())
    out.close()
  }
}
