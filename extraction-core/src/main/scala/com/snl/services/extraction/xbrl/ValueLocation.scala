package com.snl.services.extraction.xbrl

import org.apache.commons.math3.ml.clustering._ 

/**
 * A Location, used in json
 */
case class ValueLocation ( value: String, x: Double, y: Double, location: String ) extends Clusterable  {
  
  /**
   * Generate a tuple
   */
  def tuple = ( x, y )
  
  /**
   * Return a point
   */
  def getPoint = Array( x, y )
  
}
