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
package org.apache.gearpump.experiments.kafka_hdfs_pipeline

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.StreamApp
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.util.LogUtil
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}
import org.slf4j.Logger
import upickle._

class PipeLineSpec extends PropSpec with PropertyChecks with Matchers with BeforeAndAfterAll {
  val LOG: Logger = LogUtil.getLogger(getClass)
  val pipeLinePath = "conf/pipeline.conf.template"
  val pipeLineConfig = ConfigFactory.parseFile(new java.io.File(pipeLinePath))
  val kafkaConfig = KafkaConfig(pipeLineConfig)
  implicit var system: ActorSystem = null

  override def beforeAll(): Unit = {
    system = ActorSystem("PipeLineSpec")
  }

  override def afterAll(): Unit = {
    system.shutdown()
  }

  property("StreamApp should readFromKafka") {
    val app = new StreamApp("PipeLine", system, UserConfig.empty)
    app.readFromKafka(kafkaConfig, msg => {
      val jsonData = msg.msg.asInstanceOf[String]
      read[SpaceShuttleMessage](jsonData)
    }, 1, "space-shuttle-data-producer").flatMap(spaceShuttleMessage => {
      Some(upickle.read[Array[Float]](spaceShuttleMessage.body))
    }).map(scoringVector => {

    })
  }
}

