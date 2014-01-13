#!/bin/bash

# TODO: make sure you have set up dn.txt with your DataNode hostnames, 1 per line
set -e

cat dn.txt | while read h
do
  scp -rp $HOME/tpcds-kit $h:$HOME
  scp -rp $HOME/impala-tpcds-kit $h:$HOME
done
