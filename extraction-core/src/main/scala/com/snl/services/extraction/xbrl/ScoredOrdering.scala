package com.snl.services.extraction.xbrl

/**
 * Ordering for a scored tuple
 */
class ScoredOrdering[T] extends Ordering[(T,Double)] {
  
  /**
   * Compare on the first element, in descending order
   */
  def compare( x: (T,Double), y: (T,Double)) = y._2.compare( x._2 )

}