#!/bin/bash
source ./tpcds-env.sh

# find out what our node number is
source ./nodenum.sh

count=$DSDGEN_THREADS_PER_NODE

start=(NODENUM-1)*$count+1

for t in $facts
do
   for (( c=$start; c<($count+$start); c++ ))
   do
     echo "uploading part $c of ${DSDGEN_TOTAL_THREADS}"
       hdfs dfs -put ${HOME}/${t}_${c}_${DSDGEN_TOTAL_THREADS}.dat ${FLATFILE_HDFS_ROOT}/${t}/${t}_${c}_${DSDGEN_TOTAL_THREADS}.dat &
   done
done
wait

