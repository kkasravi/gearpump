package io.gearpump.experiments.yarn.glue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration

/**
 * wrapper for yarn configuration
 */
case class YarnConfig(conf: YarnConfiguration = new YarnConfiguration(new Configuration(true))) {
  def resourceManager: String = {
    conf.get("yarn.resourcemanager.hostname")
  }
}