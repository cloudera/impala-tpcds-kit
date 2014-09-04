#!/bin/bash

# TODO: make sure you have set up dn.txt with your DataNode hostnames, 1 per line
source tpcds-env.sh

n=1
cat dn.txt | while read h
do 
  echo "$h = $n"
  ssh $CLUSTER_USER@$h "echo export NODENUM=${n} > $CLUSTER_HOMEDIR/$MPP_TPCDS_DIRNAME/nodenum.sh" < /dev/null
  ((n=n+1))
done
