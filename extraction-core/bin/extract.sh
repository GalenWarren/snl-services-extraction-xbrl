#!/bin/bash

# get the script name
#SCRIPT=`basename ${BASH_SOURCE[0]}`

# vars
INPUT="/home/galen/snl-services-extraction-xbrl/extraction-core/data/sample1/input.json"
OUTPUT="/home/galen/snl-services-extraction-xbrl/extraction-core/output/sample1/output.json"
COUNT=""
OPTS=""

# parse the options
while getopts i:o:n: FLAG; do
	case $FLAG in
		i) INPUT=$OPTARG;;
		o) OUTPUT=$OPTARG;;
		n) COUNT=$OPTARG;; 
	esac
done

# build up the opts string
OPTS+=" -Dsnl.services.extraction.xbrl.input=$INPUT"
OPTS+=" -Dsnl.services.extraction.xbrl.output=$OUTPUT"
[ -z "$COUNT" ] || OPTS += " -Dsnl.services.extraction.xbrl.count=$COUNT"

# execute the command
SPARK_JAVA_OPTS=$OPTS $SPARK_HOME/bin/spark-submit \
	 	--class com.snl.services.extraction.xbrl.Extract \
	 	lib/extraction-core-0.0.1.jar
