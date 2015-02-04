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
object Score extends App with Logging {
  
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
        classOf[Configuration],
        classOf[PresentationNode],
        classOf[ValueLocation],
        classOf[ScoredVariation]
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
   * A helper to get the best combinations of value locations
   */
  private def getBestLocationCombinations(valueLocations: Array[ValueLocation], matchingSize: Int, take: Int, config: Configuration ) : Iterable[Iterable[ValueLocation]] = {
    
      Calculations.combinations( valueLocations, matchingSize)
      	.map( c => ( ScoredVariation.score(c.toSeq, config), c ))
      	.toSeq
      	.sortBy( -_._1)
      	.take(take)
      	.map( _._2 )    
  }
  
  /**
   * Run
   */
  private def run() {

    // broadcast the config
    val broadcastConfig = sparkContext.broadcast(config)
    
    // broadcast the locations map
    val broadcastLocations = sparkContext.broadcast(input.locations)
    
    // generate an rdd of locations by value -- kgw make these settings
    val locationsByValue = sparkContext.parallelize(Array(1.0,2.0)).flatMap( epsilon => {
     
      // compute the clusters for the given epsilon and minimum size -- kgw make size settings 
      Calculations.clusters( broadcastLocations.value, epsilon, 4 ).zipWithIndex
      
    }).flatMap( tuple => {
      
      // crack tuple
      val (cluster, clusterIndex) = tuple 
      
      // create one row per location, tagged with cluster index
      cluster.map( location => ( location, clusterIndex ))
      
    }).groupBy( tuple => {
      
      // crack the tuple
      val (location, clusterIndex) = tuple
      
      // group by value and cluster
      ( location.value, clusterIndex )
      
    }).map( tuple => {
      
      // crack the tuple
      val (( value, clusterIndex), locations ) = tuple
      
      // key the set by value
      (value, (locations.map( _._1 ).toArray, clusterIndex))
      
    }).cache() // kgw needed?
    
    // construct the tables map and cache it
    val tablesByValue = sparkContext.parallelize( input.tables.zipWithIndex.toSeq ).flatMap( tuple => {

      // crack tuple
      val (table, tableIndex ) = tuple
      
      // return an rdd of (tableIndex,node)
      table.map( node => ( node, tableIndex ))
      
    }).groupBy( tuple => {
      
      // crack tuple
      val (node, tableIndex ) = tuple
      
      // group by tableindex/value
      ( tableIndex, node.value )
      
    }).map( tuple => {
      
      // crack tuple
      val (( tableIndex, value), nodes) = tuple

      // make the value the key
      ( value, (nodes.map( _._1).toArray, tableIndex ))
      
    }).cache() // kgw needed?
    
    // now join together on values to create the variations
    val scoredVariations = tablesByValue.join( locationsByValue ).flatMap( tuple => {
      
      // crack tuple
      val ( value, ((nodes, tableIndex ),(locations, clusterIndex))) = tuple
      
      // determine the length on which to match
      val matchingLength = Math.min( locations.length, nodes.length)
      
      // get the combinations of locations of this length
      Calculations.combinations( locations, matchingLength ).map( combination => (combination, tableIndex ))
      
    })
    
    /*.groupBy( _._2 ).mapValues( locationsWithTableIndex => {

      // calculate the score
      0.0
      //ScoredVariation.score( locationsWithTableIndex.flatMap( _._1 ).toSeq, broadcastConfig.value)
   
    }).takeOrdered( 10 )( new ScoredOrdering())
    
    
    for (entry <- scoredVariations) {
      logger.info( "AAA %s".format( entry ))
    }*/
    
    /*
    .flatMap( locationsWithSize => {
      
      // expand the tuple
      val (locations,size) = locationsWithSize
      
      // now, for each location map, expand out to generate the variations of how this could be combined with the table values
      val matchingSize = Math.min( size, )
      
    })
    */
    
    /*
    .flatMap( cluster => {
      
      // now combine with the proper sized subsets of the table values
      
      
    })
    */
    
    /*
    // compute a map of variations by value (each map contains variations for a single value in the table, one per row)
    // exclude any value that a table says it needs but that doesn't appear in the document, we just ignore these
    val variationsByValue = table.filter( e => input.locations.contains( e._1 )).take(10).map( e => {
      
      // access the locations for this value
      val valueLocations = input.locations(e._1).toArray
      
      // determine the proper size to use for matching, which is the smaller of either the number of values to match
      // or the number of possible matching values
      val matchingSize = Math.min( valueLocations.length, e._2.length )
      
      // create the variation -- kgw make take N a parameter
      val variation = sparkContext.parallelize(Seq(e))
      	.flatMapValues( n => Calculations.combinations( n.toArray, matchingSize ))
        .flatMapValues( n => getBestLocationCombinations( broadcastLocations.value(e._1).toArray, matchingSize, 5, broadcastConfig.value ).map( n.zip( _ )))
        .map(_._2.toArray )
      	 
      // return the tuple
      ( e._1, variation.cache())
      
    })

    // map the counts
    logger.info( "AAA Counts: %s".format( variationsByValue.map( p => ( p._1,  p._2.count()))))
//    logger.info( "AAA Counts: %d".format( variationsByValue.values.map( _.count())))
    
    // compute the count
    val variationsCount = variationsByValue.values.foldLeft(1L)((count, rdd) => count * rdd.count())
    logger.info( "AAA total Count: %d".format( variationsCount ))
    */
    
    /*
    // now, take the cartesian product of value-level variations to get the table-level variations
    val variationsOption = variationsByValue.foldLeft(None: Option[RDD[List[(PresentationNode,ValueLocation)]]])((rddOption, entry) => rddOption match {
      case Some(rdd) => Some( rdd.cartesian( entry._2 ).map( p => p._1.union( p._2)))
      case None => Some( entry._2 )
    })

    // make sure we actually have some variations
    variationsOption match {
      
      case Some(variations) => {

        logger.info( "AAA Processing %d variations".format( variations.count()))

        /*
        // kgw repartition here before scoring?
        // score the variations and take the best N -- kgw set N properly
    	val scoredVariations = variations
    	  .map( v => ( ScoredVariation.score(v, broadcastConfig.value), v ))
    	  .takeOrdered( broadcastConfig.value.count )(new ScoredOrdering())
    	  .map( p => ScoredVariation( p._1, p._2.map( m => ( m._1.node, m._2.location)).toMap))
        
    	// the output object
    	val output = Output( scoredVariations.toList )
    	logger.info( "AAA %s".format(output))
        */
        
      }
      
      // this shouldn't happen, but would happen in the case that no variation were generated for some reason
      case None => throw new java.lang.IllegalStateException("No variations found!")
      
    }
    */
    
    // shut down
    logger.info( "Done, shutting down ...")
    sparkContext.stop()
      
  }
  
  /**
   * Run!
   */
  run();
  
}

