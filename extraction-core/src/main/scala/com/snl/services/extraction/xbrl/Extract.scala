package com.snl.services.extraction.xbrl

import scala.io.Source
import com.typesafe.config._
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
    	//.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    // register the kryo classes for serialization -- kgw put back
    /*
    conf.registerKryoClasses(Array(
        classOf[Candidate],
        classOf[DocumentContext],
        classOf[ScoredCandidateOrdering]
    ))*/
    	
    // set the spark master
    conf.setMaster("local[2]")
    
    // create the context
    new SparkContext(conf)
  }

  /**
   * The document context
   */
  private lazy val documentContext : DocumentContext =  {
    val source = Source.fromFile( config.context )
    try {
	  parse( source.mkString ).extract[DocumentContext]
    }
    finally {
      source.close()
    }
  }
  
  /**
   * Startup
   */
  private def execute() {
    
    // the ordering for scored candidates
	implicit val scoredCandidateOrdering = new ScoredCandidateOrdering()
	  
	// broadcast the context
	val broadcastDocumentContext = sparkContext.broadcast(documentContext) 
	  
	// score the candidates
	val scoredCandidates = sparkContext.wholeTextFiles( config.input, config.partitionCount ).values
		.map( parse(_).extract[Candidate])
		.map( c => ( c, c.score( broadcastDocumentContext.value )))
		.takeOrdered( config.count )
		.map( c => Array( c._1, c._2 ))
	
	// log the results -- kgw write to file?
	logger.info( "AAA Results: %s".format( write( scoredCandidates )))
      
    // shut down
    sparkContext.stop()
      
  }
  
  /**
   * Run!
   */
  execute();
  
}

