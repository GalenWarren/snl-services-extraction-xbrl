package com.snl.services.extraction.xbrl

import breeze.linalg._
import breeze.stats._
import com.snl.services.extraction.xbrl.PopulationMeanAndVariance._

object Math {
  
  /**
   * Measures the variance of a cluster of points, e.g. average squared distance from the mean
   */
  def variance2d( points: Iterable[(Double,Double)]) : Double = {
    
    // access the values as arrays
    val values1 = points.map( _._1 )
    val values2 = points.map( _._2 )
    
    // calculate the stats individually
    val meanAndVariance1 = meanAndVariance( values1 )
    val meanAndVariance2 = meanAndVariance( values2 )
    
    // since we're using the standard metric, we can just add these
    meanAndVariance1.populationVariance  + meanAndVariance2.populationVariance 
  }
  
  /**
   * Computes the Pearson population correlation coefficient, which measures the linearity of sequence of points
   * Based on https://gist.github.com/kaja47/6722683
   */
  def pearson( points: Iterable[(Double,Double)]) : Double = {
    
    // access the values as arrays
    val values1 = points.map( _._1 ).toArray
    val values2 = points.map( _._2 ).toArray
    
    // create the vectors
    val vector1 = DenseVector( values1 )
    val vector2 = DenseVector( values2 )
    
    // compute the means and variances
    val meanAndVariance1 = meanAndVariance( values1 )
    val meanAndVariance2 = meanAndVariance( values2 )
    
    // compute the Pearson coefficient -- kgw check this? I think this also works for population ...
    1.0 / (values1.length - 1.0) *  sum( ((vector1 - meanAndVariance1.mean) / meanAndVariance1.stdDev) :* ((vector2 - meanAndVariance2.mean) / meanAndVariance2.stdDev))
    
  }
 
}