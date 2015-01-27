package com.snl.services.extraction.xbrl

/**
 * Defines an ordering for (candidate,score) tuples that returns candidates in order of highest score first
 */
class ScoredCandidateOrdering extends Ordering[(Candidate,Double)] {

  def compare( a: (Candidate,Double), b: (Candidate,Double)) = b._2.compare(a._2)
  
}