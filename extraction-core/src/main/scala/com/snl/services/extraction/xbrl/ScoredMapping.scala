package com.snl.services.extraction.xbrl

/**
 * Represents a set of mappings and their score
 */
case class ScoredMapping ( score: Double, locations: Map[String,String]) 