#!/bin/bash

# run on a cluster node

set -ex

source tpcds-env.sh

yum -y install gcc make flex bison byacc git

cd $CLUSTER_HOMEDIR
if [ ! -d tpcds-kit ]; then
  git clone https://github.com/grahn/tpcds-kit.git
fi
cd tpcds-kit/tools
if [ ! -f tpcds-kit/tools/dsdgen ]; then
  make -f Makefile.suite
fi
