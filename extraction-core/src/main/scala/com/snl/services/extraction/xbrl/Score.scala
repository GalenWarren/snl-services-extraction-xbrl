package com.snl.services.extraction.xbrl

import scala.io.Source
import com.typesafe.config._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import grizzled.slf4j._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.math3.util.CombinatoricsUtils._

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
   * Run
   */
  private def run() {

    // broadcast the config
    val broadcastConfig = sparkContext.broadcast(config)
    
    // broadcast the input file
    val broadcastInput = sparkContext.broadcast(input)
    
    // create the location vectors, e.g. just the y coordinate and construct a k-means model. kgw could sample here ...
    val locations = sparkContext.parallelize(( 0 until input.locations.length)).cache()
    val locationVectors = locations.map(i => Vectors.dense( Array( broadcastInput.value.locations(i).y ))).cache()
    val locationsModel = KMeans.train(locationVectors, 10, 20)	// kgw make settings
    
    // now key by (clusterindex,value) -- kgw repartition here?
    // kgw check cache calls and can combine these pairs at top?
    val clusterLocationsByValue = locationsModel.predict( locationVectors )
    	.zipWithIndex
    	.groupBy( t => ( t._1, broadcastInput.value.locations(t._2.toInt).value ))
    	.mapValues( _.map( _._2.toInt ))
    	.keyBy( _._1._2 )
    	.mapValues( t => ( t._1._1, t._2.toIndexedSeq ))
    	
    // parse the input tables
    val tableNodesByValue = sparkContext.parallelize( input.tables )
    	.zipWithIndex
    	.flatMap( t1 => t1._1.zipWithIndex.map( t2 => ( ( t1._2.toInt, t2._1.value), t2._2 ) ))
    	.groupByKey
    	.keyBy( _._1._2 )
    	.mapValues( t => ( t._1._1, t._2.toIndexedSeq ))

    // join on value and cache
    val locationsNodesByValue = clusterLocationsByValue.join( tableNodesByValue ).cache()
    	
    // expand out the combinations and cache
    val locationsByClusterTableValue = locationsNodesByValue.flatMap( t => {

      // crack the tuple
      val ( value, (( clusterIndex, locations ), (tableIndex, nodes))) = t
      
      // determine the matching size
      val matchingLength = Math.min( locations.length, nodes.length )
      
      // expand out the possible mappings, apply a limit in case this is huge (kgw can better handle this?)
      val combinations = Calculations.combinations( locations, matchingLength).take( 2 ) //broadcastConfig.value.maxLocationCombinations)
      
      // yield one row for each combination, keyed by (clusterIndex, tableIndex, value)
      combinations.map( t => ((clusterIndex, tableIndex, value), t ))
      
    }).repartition(2).cache()
    
    // get the base variations object, this is keyed by (clusterIndex,tableIndex,"") and has an empty iterable for the value
    val baseVariations = locationsNodesByValue.groupBy( t => {
      
      // crack the tuple
      val ( value, (( clusterIndex, locations ), (tableIndex, nodes))) = t
      
      // map group by the indexes
      ( clusterIndex, tableIndex, "" )
      
    }).mapValues( v => Iterable[Int]())
    
    // join repeatedly against the locations by cluster/table/value to build up the variations rdd
    val variations = tableNodesByValue.keys.collect.toList.distinct.foldLeft(baseVariations)((rdd, newValue) => {
      
      // first, tranform the key of the incoming rdd so that the values match the values we'll join to
      rdd.map( t => {
        
        // crack the tuple
        val ((clusterIndex, tableIndex, oldValue), locations ) = t
        
        // change the value
        (( clusterIndex, tableIndex, newValue), locations )
        
      }).leftOuterJoin( locationsByClusterTableValue ).mapValues( t => {
        
        // crack tuple
        val ( oldLocations, newLocationsOption ) = t
        
        // if we have new locations for this value, tack them on to the old locations
        newLocationsOption match {
          case Some(newLocations) => oldLocations ++ newLocations
          case None => oldLocations
        }
        
      })
      
    })
    
    // kgw add scored variations
    
    logger.info( "AAA %d".format( variations.count ))
    
    /*
    for (entry <- variations.collect()) {
      logger.info( "AAA %s".format( entry ))
    }
    */
    
//    .join( tableNodesByValue.keys )
    
    /*
    .mapValues( valueMappings => {
      
      valueMappings.map( _._1 ).toList.distinct
      
    }).collect.map( t => {
      
      // unpack the tuple
      val (( clusterIndex, tableIndex), values ) = t
      
      // process each value
      values.foldLeft(None:Option[RDD[((Int,Int,String),Iterable[Int])]])( (rddOption, value) => {
        
	    rddOption match {
	    
          // first time through, just use the rdd of locations for the first value
	      case None => Some(locationsByClusterTableValue.filter( _._1 == value))
	      
	      // subsequent time through, so we'll join here
	      case Some(rdd) => rdd.map( t => {
	        
	        // unpack tuple
	        val ((clusterIndex2, tableIndex2, value2), locations ) = t
	        
	        // swap in the target value
	        (( clusterIndex2, tableIndex2, value ), locations )
	        
	      }).join( locationsByClusterTableValue )
	        
	    }
        
      })
      
    })
    */

    /*
    // loop through the possible mappigs
    for (( clusterIndex, tableIndex ) <- valuesByClusterTable) {
      
    }
    */
    
    /*
    	.keyBy( t => {
    	  
    	  // crack the tuple
    	  val ( value, (( clusterIndex, locations ), (tableIndex, nodes))) = t
    	  
    	  (clusterIndex,tableIndex,value)
    	  //(( clusterIndex, tableIndex, value ), (locations, nodes))
    	  
    	})
    	*/
    
    	/*
    	.map( t => {

    	  // crack the tuple
    	  val (( clusterIndex, tableIndex ), ( value, (( clusterIndex2, locations ), (tableIndex2, nodes)))) = t
    	  
    	  // keep just the location and node indexes
    	  (( clusterIndex, tableIndex, value ), (locations, nodes))
    	  
    	})*/
    
    
    	
    /*
    // generate an rdd of locations by value -- kgw make these settings
    val locationsByValue = sparkContext.parallelize(Array(1.0)).flatMap( epsilon => {
     
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
      
    });
    
    // generate a map of tables by index and value
    val tablesByIndexAndValue = sparkContext.parallelize( input.tables.zipWithIndex.toSeq ).flatMap( tuple => {

      // crack tuple
      val (table, tableIndex ) = tuple
      
      // return an rdd of (tableIndex,node)
      table.map( node => ( node, tableIndex ))
      
    }).groupBy( tuple => {
      
      // crack tuple
      val (node, tableIndex ) = tuple
      
      // group by tableindex/value
      ( tableIndex, node.value )
      
    }).cache()
    
    // key the tables just by value, to prepare for joining to construct variations
    val tablesByValue = tablesByIndexAndValue.map( tuple => {
      
      // crack tuple
      val (( tableIndex, value), nodes) = tuple

      // make the value the key
      ( value, (nodes.size, tableIndex ))
      
    });
    
    // now join together on values to create the variations
    val locationsByTableAndValue = tablesByValue.join( locationsByValue ).flatMap( tuple => {
      
      // crack tuple
      val ( value, ((nodeCount, tableIndex),(locations, clusterIndex))) = tuple
      
      // determine the length on which to match
      val matchingLength = Math.min( locations.length, nodeCount)
      
      // get the combinations of locations of this length, but with a cap to prevent things from running away
      Calculations.combinations( locations, matchingLength )
      	.take(config.maxLocationCombinations)
      	.map( combination => ((tableIndex, value), combination))
      
    }).cache()
    
    
    /*.groupBy( _._2 ).mapValues( locationsWithTableIndex => {

      // calculate the score
      0.0
      //ScoredVariation.score( locationsWithTableIndex.flatMap( _._1 ).toSeq, broadcastConfig.value)
   
    }).takeOrdered( 10 )( new ScoredOrdering())
    */
    
    // loop through the tables and values
    val variations = tablesByIndexAndValue.keys.collect.foldLeft(None:Option[RDD[((Int,String),(Iterable[ValueLocation]))]])( (rddOption, key) => {

      // construct an rdd for this table/value combination
      val locationCombination = locationsByTableAndValue.filter( r => r._1 == key )
      
      // now build up the rdd
      rddOption match {
             
        // first time through, so just take the rdd for this key 
        case None => Some( locationCombination )
      
        // not first time through, so must join and map
        case Some(rdd) => Some({
          
          val x = rdd.map( tuple => {
            
            // crack tuple
            val ((tableIndex, value), locations) = tuple
            
            // map it, changing the value to the value we're mapping to so that the join will work
            ( (tableIndex, value), locations )
            
          }).join( locationCombination ).groupByKey()
          
          x
          
        })
      }
    })

    /*
    for (entry <- variations.collect()) {
      logger.info( "AAA %s".format( entry ))
    }
    */
    */
    
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

