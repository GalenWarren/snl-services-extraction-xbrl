package com.snl.services.extraction.xbrl

/**
 * Represents an xbrl presentation node in the input
 */
case class PresentationNode( node: String, groups: Array[String], value: String, tableIndex: Int )