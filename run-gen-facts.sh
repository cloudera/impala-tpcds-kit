#!/bin/bash

# TODO: make sure you have set up dn.txt with your DataNode hostnames, 1 per line

cat dn.txt | while read h
do 
  ssh $h "export HOME=/mnt/sdg1/; cd $HOME/impala-tpcds-kit; ./gen-facts.sh" < /dev/null &
done
