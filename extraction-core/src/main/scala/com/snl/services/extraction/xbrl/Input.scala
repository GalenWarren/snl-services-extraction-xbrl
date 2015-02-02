package com.snl.services.extraction.xbrl

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * The input structure
 */
case class Input( tables: Map[String,Map[String,List[PresentationNode]]], locations: Map[String,List[Location]]) 