#!/bin/bash
CLASS=$1
FILE=$2
MODEL=$3
THRESHOLD=$4
/home/bigdata/Software/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class $CLASS --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar $FILE $MODEL $THRESHOLD