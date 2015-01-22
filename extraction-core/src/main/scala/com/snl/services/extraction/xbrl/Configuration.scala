package com.snl.services.extraction.xbrl

import akka.actor._
import akka.util._
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.typesafe.config._

/**
 * The configuration extension
 */
class Configuration ( config: Config ) extends Extension {

  /**
   * The name of the application
   */
  val appName = config.getString("snl.services.extraction.xbrl.appName")
  
}

object Configuration extends ExtensionId[Configuration] with ExtensionIdProvider {

  /**
   * Lookup value for this extension
   */
  override def lookup = Configuration
  
  /**
   * Create the extension
   */
  override def createExtension( system: ExtendedActorSystem ) = new Configuration( system.settings.config )
  
  /**
   * Helper to get a duration from a config file
   */
  def getDuration( config: Config, name: String, unit: TimeUnit = TimeUnit.MILLISECONDS ) = Duration( config.getDuration(name, unit), unit )
}