#!/bin/sh
###################################################################################
#This script merges HBase Table. Takes two paramters as input
# arg1 = HBase Table Name
# arg2 = Merge adjacent Regions only {true/false}. As of now only true is supported.
###################################################################################
tableName=$1
mergeAdj=$2
spark-submit --driver-class-path "/usr/hdp/hadoop/lib/*:/usr/hdp/hbase/lib/*:/usr/hdp/hadoop/*" --class com.aquaifer.HBaseMergePartitions HBaseUtility-0.0.1-SNAPSHOT.jar $tableName $mergeAdj
