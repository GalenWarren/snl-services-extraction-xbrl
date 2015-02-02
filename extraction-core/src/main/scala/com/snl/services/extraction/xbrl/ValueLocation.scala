package com.snl.services.extraction.xbrl

/**
 * A Location, used in json
 */
case class ValueLocation ( x: Double, y: Double, location: String ) {
  
  /**
   * Generate a tuple
   */
  def tuple = ( x, y )
  
}
