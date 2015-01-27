#!/bin/bash

# get the script name
#SCRIPT=`basename ${BASH_SOURCE[0]}`

# vars
CONTEXT="/home/galen/snl-services-extraction-xbrl/extraction-core/data/sample1/context.json"
INPUT="/home/galen/snl-services-extraction-xbrl/extraction-core/data/sample1/candidate*.json"
OUTPUT="/home/galen/snl-services-extraction-xbrl/extraction-core/output/sample1/scored.json"
COUNT=""
OPTS=""

# parse the options
while getopts c:i:o:n: FLAG; do
	case $FLAG in
		c) CONTEXT=$OPTARG;;
		i) INPUT=$OPTARG;;
		o) OUTPUT=$OPTARG;;
		n) COUNT=$OPTARG;; 
	esac
done

# build up the opts string
OPTS+=" -Dsnl.services.extraction.xbrl.context=$CONTEXT"
OPTS+=" -Dsnl.services.extraction.xbrl.input=$INPUT"
OPTS+=" -Dsnl.services.extraction.xbrl.output=$OUTPUT"
[ -z "$COUNT" ] || OPTS += " -Dsnl.services.extraction.xbrl.count=$COUNT"

# execute the command
SPARK_JAVA_OPTS=$OPTS $SPARK_HOME/bin/spark-submit \
	 	--class com.snl.services.extraction.xbrl.Extract \
	 	lib/extraction-core-0.0.1.jar
