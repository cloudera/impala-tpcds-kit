#!/bin/bash

# TODO: make sure you have set up dn.txt with your DataNode hostnames, 1 per line

n=1
cat dn.txt | while read h
do 
  echo "$h = $n"
  ssh $h "echo export NODENUM=${n} > $HOME/impala-tpcds-kit/nodenum.sh" < /dev/null
  ((n=n+1))
done
