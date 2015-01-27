package com.snl.services.extraction.xbrl

/**
 * Represents a point in 2d space
 */
trait Point {
  
  /**
   * The x coordinate
   */
  val x : Double
  
  /**
   * The y coordinate
   */
  val y: Double
  
}

object Point {
  
  /**
   * Measures the compactness of a sequence of points
   */
  def compactness( points: Iterable[Point]) : Double = {
    0
  }
  
  /**
   * Measures the linearity of sequence of points
   */
  def linearity( points: Iterable[Point]) : Double = {
    0
  }
  
}