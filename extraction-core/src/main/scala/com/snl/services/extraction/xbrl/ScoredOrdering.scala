package com.snl.services.extraction.xbrl

/**
 * Ordering for a scored tuple
 */
class ScoredOrdering[T] extends Ordering[(Double,T)] {
  
  /**
   * Compare on the first element, in descending order
   */
  def compare( x: (Double,T), y: (Double,T)) = y._1.compare( x._1 )

}