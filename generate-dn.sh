#!/bin/bash

set -e

AWS_CLUSTERNAME="mpp_benchmark_vertica"
DATANODE_FILE="dn.txt"

# get all instances with the tag ClusterName=${AWS_CLUSTERNAME}, and search for the PublicIpAddress line.
aws ec2 describe-instances --filter Name=tag:ClusterName,Values=${AWS_CLUSTERNAME} | \
  sed -n -e 's/\"PublicIpAddress\": "\(.*\)",/\1/p' | tr -d ' ' > ${DATANODE_FILE}
