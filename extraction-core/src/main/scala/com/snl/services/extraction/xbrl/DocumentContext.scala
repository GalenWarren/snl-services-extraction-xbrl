package com.snl.services.extraction.xbrl

/**
 * The document context, holds xbrl contexts and locations
 */
case class DocumentContext( xbrlContextMap: Map[String,XbrlContext], locationMap: Map[String,DocumentLocation]) {
  
  /**
   * Aux constructor for parsing json
   */
  def this( xbrlContexts: List[XbrlContext], locations: List[DocumentLocation]) {
    this( xbrlContexts.map( c => ( c.name, c )).toMap, locations.map( l => ( l.location, l )).toMap )
  }
  
}