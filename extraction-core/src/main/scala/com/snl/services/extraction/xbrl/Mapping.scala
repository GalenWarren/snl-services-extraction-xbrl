package com.snl.services.extraction.xbrl

/**
 * A class that represents a mapping of an xbrl concept/context to a location in the document
 */
case class Mapping( concept: String, context: String, location: String, groups: List[String]) {
  
  /**
   * Aux constructor to allow deserializing with not location specified
   */
  def this( concept: String, context: String, groups: List[String] ) { 
    this( concept, context, null, groups ) 
  }
  
  
  
}