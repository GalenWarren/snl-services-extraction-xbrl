package com.snl.services.extraction.xbrl

import akka.actor._

/**
 * The main actor for the extraction 
 */
class Main extends Actor {
  
  import context._
  
  /**
   * The configuration instance
   */
  private val config = Configuration(system)

  /**
   * Message handler
   */
  def receive = {
    case _ => {}
  }
  
}