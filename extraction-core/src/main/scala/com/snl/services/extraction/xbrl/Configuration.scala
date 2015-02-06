package com.snl.services.extraction.xbrl

import akka.actor._
import akka.util._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.typesafe.config._

/**
 * The configuration extension
 */
class Configuration ( config: Config ) {

  /**
   * The name of the application
   */
  val appName = config.getString("snl.services.extraction.xbrl.appName")
  
  /**
   * The mapping candidates
   */
  val input = config.getString("snl.services.extraction.xbrl.input")
  
  /**
   * The output file to write the scored candidates to
   */
  val output = config.getString("snl.services.extraction.xbrl.output")
  
  /**
   * The number of scored candidates to return
   */
  val count = config.getInt("snl.services.extraction.xbrl.count")
  
  /**
   * The number of partitions to use for processing candidates
   */
  val partitionCount = config.getInt("snl.services.extraction.xbrl.partitionCount")
  
  /**
   * The maximum number of location combinations we'll process
   */
  val maxLocationCombinations = config.getInt("snl.services.extraction.xbrl.maxLocationCombinations")
}

