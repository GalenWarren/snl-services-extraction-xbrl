package com.snl.services.extraction.xbrl

import grizzled.slf4j._

/**
 * A class that iterates through combinations of underlying iterator values
 */
class CombinationIterator[T: Manifest]( iterables: Array[Iterable[T]]) extends Iterator[Array[T]] with Logging {

  /**
   * The array of iterators
   */
  private val iterators = new Array[Iterator[T]]( iterables.length)
  
  /**
   * The current values
   */
  private val values = new Array[T]( iterables.length )
  
  /**
   * Tracks whether the next entry is valid
   */
  private var hasNextValue : Boolean = true

  /**
   * Resets an iterator at the given position
   */
  private def resetIterator( index: Int ) {
    
    // recreate the iterator
    iterators(index) = iterables(index).iterator
    
    // check if this iterator has a next value (it should!) and update next is valid
    val iterableHasNextValue = iterators(index).hasNext
    if (iterableHasNextValue) {
      
      // when next is valid, get the next value
      values(index) = iterators(index).next()
      
    }
    else {
      
      // didn't get a next value
      hasNextValue = false
      logger.warn( "hasNext returned false for index %d".format( index ))
    }
    
  } 
  
  /**
   * Moves next at the given position
   */
  private def moveNext( index: Int = 0 ) {
    
    // can we move forward at this index?
    if (iterators(index).hasNext) {
      
      //logger.info("has next value at index %d".format( index ))
      
      // yes, move next and update the value
      values(index) = iterators(index).next()
      
    }
    else if ((index + 1) < values.length) {
      
      //logger.info("resetting at index %d".format( index ))
      
      // no, but we're not the last one, so reset this one
      resetIterator( index )
      
      // and move at the next index position
      moveNext( index + 1 )
      
    }
    else {

      //logger.info("done at index %d".format( index ))
      
      // we have nothing left to move forward, so mark that there are no more combinations 
      hasNextValue = false
      
    }
    
  }

  /**
   * Indicates whether this iterator has a next value
   */
  def hasNext: Boolean = {
    
    //logger.info( "returning %s from hasNext: (%s)".format( hasNextValue, values.mkString(",")))
   
    hasNextValue
  }

  /**
   * Returns the next value and advances
   */
  def next(): Array[T] = {
   
    
    // clone the current values, this is what we'll return
    val result = values.clone

//    logger.info( "called next %d: %s".format( result.length, result.mkString(",") ))
    
    // advance the combination
    moveNext()
    
    // done
    result
  }
  
  /**
   * Initialize
   */
  for (index <- 0 until iterables.length) {
    
    if (!iterables(index).iterator.hasNext) {
      logger.info( "AAA %d".format( index ))
    }
    
    resetIterator( index )
  }

}