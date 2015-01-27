package com.snl.services.extraction.xbrl

import scala.language.implicitConversions
import breeze.stats._

/**
 * Extends MeanAndVariance to support population metrics
 */
class PopulationMeanAndVariance( meanAndVariance: MeanAndVariance) {

  /**
   * Return the population variance
   */
  lazy val populationVariance = meanAndVariance.variance * (meanAndVariance.count - 1 ) / meanAndVariance.count
  
}

object PopulationMeanAndVariance {
  
  def apply( meanAndVariance: MeanAndVariance ) = new PopulationMeanAndVariance( meanAndVariance )
  
  implicit def MeanAndVariance2PopulationMeanAndVariance( meanAndVariance: MeanAndVariance) = PopulationMeanAndVariance( meanAndVariance )
  
}