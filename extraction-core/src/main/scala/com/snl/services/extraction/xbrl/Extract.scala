package com.snl.services.extraction.xbrl

import scala.io.Source
import com.typesafe.config._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import grizzled.slf4j._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

/**
 * The main actor for the extraction 
 */
object Extract extends App with Logging {
  
  /**
   * The configuration instance
   */
  private val config = new Configuration( ConfigFactory.load())

  /**
   * The spark context
   */
  private lazy val sparkContext : SparkContext = {
    
    // the config
    val conf = new SparkConf()
    	.setAppName(config.appName)
    	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    // register the kryo classes for serialization
    conf.registerKryoClasses(Array(
        classOf[Configuration]
    ))
    	
    // set the spark master -- kgw change to be able to run on cluster
    conf.setMaster("local[2]")
    
    // create the context
    new SparkContext(conf)
  }

  /**
   * The input
   */
  private lazy val input : Input =  {
    val source = Source.fromFile( config.input )
    try {
	  parse( source.mkString ).extract[Input]
    }
    finally {
      source.close()
    }
  }

  /**
   * Scores a permutation
   */
  def scoreVariation( variation: Seq[(PresentationNode,Location)], config: Configuration ) : Double =  {
    0.0
  }
  
  /**
   * Run
   */
  private def run() {

    // broadcast the config
    val broadcastConfig = sparkContext.broadcast(config)
    
    // broadcast the locations
    val broadcastLocations = sparkContext.broadcast(input.locations)
    
    // access the main table -- kgw change to handle multiple tables!
    val tableIds = input.tables.keys.toSeq
    val table = input.tables(tableIds(0))

    // compute a map of variations value (each map contains variations for a single value in the table, one per row)
    val variationsByValue = table.map( e => ( e._1, sparkContext.parallelize(Seq(e))
        .flatMapValues( n => Calculations.variations( broadcastLocations.value(e._1).toArray, n.length).map(n.zip(_)))
        .map( _._2)
    )) 
    
    // now, take the cartesian product of value-level variations to get the table-level variations
    val variationsOption = variationsByValue.foldLeft(None: Option[RDD[List[(PresentationNode,Location)]]])((rddOption, entry) => rddOption match {
      case Some(rdd) => Some( rdd.cartesian( entry._2 ).map( p => p._1.union( p._2)))
      case None => Some( entry._2 )
    })

    // make sure we actually have some variations
    variationsOption match {
      
      case Some(variations) => {

        // kgw repartition here before scoring?
        // score the variations and take the best N -- kgw set N properly
    	val scoredMappings = variations
    	  .map( v => ( scoreVariation(v, broadcastConfig.value), v ))
    	  .takeOrdered(2)(new ScoredOrdering())
    	  .map( p => ScoredMapping( p._1, p._2.map( m => ( m._1.node, m._2.location)).toMap))
        
    	// the output object
    	val output = Output( scoredMappings.toList )
    	logger.info( "AAA %s".format(output))
        
      }
      
      // this shouldn't happen, but would happen in the case that no variation were generated for some reason
      case None => throw new java.lang.IllegalStateException("No variations found!")
      
    }
    
    // shut down
    logger.info( "Done, shutting down ...")
    sparkContext.stop()
      
  }
  
  /**
   * Run!
   */
  run();
  
}

