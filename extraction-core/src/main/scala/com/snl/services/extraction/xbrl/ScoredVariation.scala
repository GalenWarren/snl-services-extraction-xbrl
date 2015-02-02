package com.snl.services.extraction.xbrl

/**
 * Represents a set of mappings of presentation nodes to locations and the score
 */
case class ScoredVariation ( score: Double, locations: Map[String,String]) 

object ScoredVariation {
  
  /**
   * Scores a variation
   */
  def score( variation: Seq[(PresentationNode,ValueLocation)], config: Configuration ) : Double =  {
    
    // calculate the compactness of the locations in this variation 
    Calculations.compactness( variation.map( _._2.tuple))
        
  }  
  
}