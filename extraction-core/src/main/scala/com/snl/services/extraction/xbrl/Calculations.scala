package com.snl.services.extraction.xbrl

import org.apache.commons.math3.stat.descriptive._
import org.apache.commons.math3.stat.correlation._
import org.apache.commons.math3.ml.clustering._ 
import org.paukov.combinatorics._
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
   * Computes the weighted average of a sequence of (value,weight) tuples 
   */
  def weightedAverage( values: Seq[(Double,Double)]) : Double = values.map( _._1 ).sum / values.map( _._2 ).sum
  
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
  
  /**
   * Returns combinations of a set of elements of a given size
   */
  def combinations[T <: AnyRef ]( elements: Array[T], size: Int ) : Iterable[Iterable[T]] = {
    for {
      combination <- Factory.createSimpleCombinationGenerator( Factory.createVector( elements ), size).asScala
    } yield combination.asScala
  }
  
  /**
   * Returns the variations of the given size of a set of elements -- kgw need to handle case where size > elements.length, e.g. must pad
   * and returns Option[T] in the inner array, not T
   */
  def variations[T <: AnyRef ]( elements: Array[T], size: Int ) : Iterable[Iterable[T]] = {

    for {
      combination <- Factory.createSimpleCombinationGenerator( Factory.createVector( elements ), size).asScala
      permutation <- Factory.createPermutationGenerator(combination).asScala
    } yield permutation.asScala
  }
  
  /**
   * Generates clusters with a given epsilon and minimum points, using DBSCAN
   */
  def clusters[ T <: Clusterable ]( elements: Iterable[T], epsilon: Double, minimumPoints: Int ) : Iterable[Iterable[T]] = {
    
    // generate the clusters
    val clusterer = new DBSCANClusterer[T]( epsilon, minimumPoints, new VerticalDistance())
    val clusters = clusterer.cluster( elements.asJavaCollection).asScala
    
    // convert to the return type
    clusters.map( cluster => cluster.getPoints().asScala.toIterable).toIterable
  }
  
}