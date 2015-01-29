package com.snl.services.extraction.xbrl

import org.apache.commons.math3.stat.descriptive._
import org.apache.commons.math3.stat.correlation._
import scala.collection.JavaConverters._

object Calculations {
  
  /**
   * Helper to create summary statistics for a series of values 
   */
  private def summaryStatistics( values: Iterable[Double]) = values.foldLeft(new SummaryStatistics())( (stats, value) => { 
    stats.addValue( value )
    stats 
  })
  
  /**
   * Measures the variance of a cluster of points, e.g. average squared distance from the mean
   */
  def variance2d( points: Iterable[(Double,Double)]) : Double = {

    // compute the summary statistics
    val stats1 = summaryStatistics( points.map( _._1 ))
    val stats2 = summaryStatistics( points.map( _._2 ))
    
    // since we're using the standard metric, we can just add these
    stats1.getPopulationVariance() + stats2.getPopulationVariance()
  }
  
  /**
   * Calculates the compactness of a set of points on a scale from 0 to 1 (1 is most compact)
   */
  def compactness( points: Iterable[(Double,Double)], k: Double = 1.0 ) : Double = {
    val variance = variance2d( points )
    1.0 / ( 1 + Math.pow( variance, k ))
  }
  
  /**
   * Computes the Pearson population correlation coefficient, which measures the linearity of sequence of points
   */
  def pearson( points: Iterable[(Double,Double)]) : Double = 
    new PearsonsCorrelation().correlation( points.map( _._1 ).toArray, points.map( _._2 ).toArray)
  
  /**
   * Determines the linearity of a set of points on a scale from 0 to 1
   */
  def linearity( points: Iterable[(Double,Double)] ) : Double = pearson( points ) match {
    case n : Double if n.isNaN => 1
    case v : Double => v * v
  }
  
}