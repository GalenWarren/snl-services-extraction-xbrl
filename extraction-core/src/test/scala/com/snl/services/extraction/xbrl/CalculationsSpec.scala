package com.snl.services.extraction.xbrl

import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CalculationsSpec extends FunSpec with Matchers with Assertions {

  /**
   * Some tests to ensure variance calculations are working properly
   */
  describe( "The 2d variance") {

    it ("should be 0.0 for a single point") {
      Calculations.variance2d( Seq((0.0,0.0))) should be (0.0)
    }
    
    it ("should be 1.0 for a line of length 2") {
      Calculations.variance2d( Seq((0.0,0.0),(2.0,0.0))) should be (1.0)
    }
    
    it ("should be 2.0 for a square with sides of length 2") {
      Calculations.variance2d( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (2.0)
    }
    
  }
  
  /**
   * Some tests to ensure compactness calculations are working properly
   */
  describe( "The compactness") {

    it ("should be 1.0 for a single point") {
      Calculations.compactness( Seq((0.0,0.0))) should be (1.0)
    }
    
    it ("should be 0.5 for a two-point line of length 2") {
      Calculations.compactness( Seq((0.0,0.0),(2.0,0.0))) should be (0.5)
    }
    
    it ("should be 0.2 for a two-point line of length 4") {
      Calculations.compactness( Seq((0.0,0.0),(0.0,4.0))) should be (0.2)
    }
    
    it ("should be 1/3 for a square with sides of length 2") {
      Calculations.compactness( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (1.0/3.0)
    }
    
  }
  
  /**
   * Some tests to ensure that our calculation of the Pearson coefficient is working properly
   */
  describe( "The Pearson coefficient" ) {
    
    it ("should be 1.0 for three collinear points with slope 1") {
      Calculations.pearson( Seq((0.0,0.0),(1.0,1.0),(2.0,2.0))) should be (1.0)
    }
    
    it ("should be -1.0 for three collinear points with slope -1") {
      Calculations.pearson( Seq((0.0,0.0),(-1.0,1.0),(-2.0,2.0))) should be (-1.0)
    }
    
    it ("should be NaN for three horizontal collinear points") {
      assert(Calculations.pearson( Seq((0.0,0.0),(1.0,0.0),(2.0,0.0))).isNaN)
    }
    
    it ("should be NaN for three vertical points") {
      assert(Calculations.pearson( Seq((0.0,0.0),(0.0,1.0),(0.0,2.0))).isNaN)
    }
    
    it ("should be 0.0 for a square of points") {
      Calculations.pearson( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (0.0)
    }
    
  }
  
  /**
   * Some tests to ensure that our calculation of linearity is working properly
   */
  describe( "The linearity" ) {
    
    it ("should be 1.0 for three collinear points with slope 1") {
      Calculations.linearity( Seq((0.0,0.0),(1.0,1.0),(2.0,2.0))) should be (1.0)
    }
    
    it ("should be -1.0 for three collinear points with slope -1") {
      Calculations.linearity( Seq((0.0,0.0),(-1.0,1.0),(-2.0,2.0))) should be (1.0)
    }
    
    it ("should be 1.0 for three horizontal collinear points") {
      Calculations.linearity( Seq((0.0,0.0),(1.0,0.0),(2.0,0.0))) should be (1.0)
    }
    
    it ("should be 1.0 for three vertical points") {
      Calculations.linearity( Seq((0.0,0.0),(0.0,1.0),(0.0,2.0))) should be (1.0)
    }
    
    it ("should be 0.0 for a square of points") {
      Calculations.linearity( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (0.0)
    }
    
  }

  /**
   * Test variations
   */
  describe( "Variations" ) {
    
    it ("should be generated properly for a 4 element set taken 4 at a time") {
      val variations = Calculations.variations( Array("a","b","c","d"), 4 ).toSeq
      variations.length should be (24)
    }
    
    it ("should be generated properly for a 4 element set taken 3 at a time") {
      val variations = Calculations.variations( Array("a","b","c","d"), 3 ).toSeq
      variations.length should be (24)
    }
    
    it ("should be generated properly for a 4 element set taken 2 at a time") {
      val variations = Calculations.variations( Array("a","b","c","d"), 2 ).toSeq
      variations.length should be (12)
    }
    
    it ("should be generated properly for a 4 element set taken 1 at a time") {
      val variations = Calculations.variations( Array("a","b","c","d"), 1 ).toSeq
      variations.length should be (4)
    }
    
  }
  
  
}
