package com.snl.services.extraction.xbrl

/**
 * Represents a set of mappings of presentation nodes to locations and the score
 */
case class ScoredVariation ( score: Double, locations: Map[String,String]) 

object ScoredVariation {
  
  /**
   * Scores a set
   */
  def score( set: Seq[ValueLocation], config: Configuration ) : Double =  {
    
    // calculate the compactness of the locations in this variation 
    Calculations.compactness( set.map( _.tuple))
        
  }  
  
}