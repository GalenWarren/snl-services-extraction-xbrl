package com.snl.services.extraction.xbrl

import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MathSpec extends FunSpec with Matchers with Assertions {
  
  /**
   * Some tests to ensure that our calculation of the Pearson coefficient is working properly
   */
  describe( "The Pearson coefficient" ) {
    
    it ("should be 1.0 for three collinear points with slope 1") {
      Math.pearson( Seq((0.0,0.0),(1.0,1.0),(2.0,2.0))) should be (1.0)
    }
    
    it ("should be -1.0 for three collinear points with slope 1") {
      Math.pearson( Seq((0.0,0.0),(-1.0,1.0),(-2.0,2.0))) should be (-1.0)
    }
    
    it ("should be NaN for three horizontal collinear points") {
      assert(Math.pearson( Seq((0.0,0.0),(1.0,0.0),(2.0,0.0))).isNaN)
    }
    
    it ("should be NaN for three vertical points") {
      assert(Math.pearson( Seq((0.0,0.0),(0.0,1.0),(0.0,2.0))).isNaN)
    }
    
    it ("should be 0.0 for a square of points") {
      Math.pearson( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (0.0)
    }
    
  }
  
  /**
   * Some tests to ensure variance calculations are working properly
   */
  describe( "The 2d variance") {

    it ("should be 0.0 for a single point") {
      Math.variance2d( Seq((0.0,0.0))) should be (0.0)
    }
    
    it ("should be 1.0 for a line of length 2") {
      Math.variance2d( Seq((0.0,0.0),(2.0,0.0))) should be (1.0)
    }
    
    it ("should be 2.0 for a square with sides of length 2") {
      Math.variance2d( Seq((1.0,1.0),(1.0,-1.0),(-1.0,1.0),(-1.0,-1.0))) should be (2.0)
    }
    
  }

}
