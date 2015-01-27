package com.snl.services.extraction.xbrl

/**
 * A document location
 */
case class DocumentLocation( location: String, x: Double, y: Double ) {
  
  /**
   * Return the point as a tuple 
   */
  def point = ( x, y )
  
}