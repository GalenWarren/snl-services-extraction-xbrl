package com.snl.services.extraction.xbrl

import org.apache.commons.math3.ml.distance._ 

/**
 * A class that implements measurements in the vertical distance but ignores horizontal separation
 */
class VerticalDistance extends DistanceMeasure  {

  /**
   * Computes the vertical distance between two points
   */
  def compute( a: Array[Double], b: Array[Double]) = Math.abs(a(1) - b(1))
  
}