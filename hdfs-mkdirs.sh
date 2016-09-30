#!/bin/bash
source ./tpcds-env.sh

# make the table level dir
hdfs dfs -mkdir ${FLATFILE_HDFS_ROOT}

# make a directory for each table
for t in $dims $facts
do 
  echo "making HDFS directory ${FLATFILE_HDFS_ROOT}/${t}"
  hdfs dfs -mkdir ${FLATFILE_HDFS_ROOT}/${t}
done

echo "HDFS directories:"
hdfs dfs -ls ${FLATFILE_HDFS_ROOT}
