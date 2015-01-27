package com.snl.services.extraction.xbrl

/**
 * A candidate mapping
 */
case class Candidate( values: List[CandidateValue], tables: List[CandidateTable]) {
  
  /**
   * Generates a score for this candidate given a document context 
   */
  def score( documentContext: DocumentContext ) : Double = {
    
    Math.pearson(documentContext.locationMap.values.map( _.point ))
    0
  }
  
}