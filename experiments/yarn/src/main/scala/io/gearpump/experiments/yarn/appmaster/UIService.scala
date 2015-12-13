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

import akka.actor._
import com.typesafe.config.ConfigFactory
import io.gearpump.services.main.Services
import io.gearpump.transport.HostPort
import io.gearpump.util.{Constants, LogUtil}

class UIService(masters: List[HostPort], host: String, port: Int) extends Actor {
  private val LOG = LogUtil.getLogger(getClass)
  
  override def preStart(): Unit = {
    val mastersArg = masters.mkString(",")
    LOG.info(s"Launching services -master $mastersArg")
    System.setProperty(Constants.GEARPUMP_SERVICE_HOST, host)
    System.setProperty(Constants.GEARPUMP_SERVICE_HTTP, port.toString)
    System.setProperty("akka.remote.netty.tcp.hostname", host)

    System.setProperty(Constants.GEARPUMP_HOSTNAME, host)
    for (index <- 0 until masters.length) {
      val masterHostPort = masters(index)
       System.setProperty(s"${Constants.GEARPUMP_CLUSTER_MASTERS}.$index", s"${masterHostPort.host}:${masterHostPort.port}")
    }

    ConfigFactory.invalidateCaches()
    Services.main(Array.empty[String])
  }

  override def receive: Receive = {
    case _ =>
      LOG.error(s"Unknown message received")
  }
}