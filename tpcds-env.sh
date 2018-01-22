# path to the tpcds-kit directory
export TPCDS_ROOT=$HOME/tpcds-kit

# top level directory for flat files in HDFS
export FLATFILE_HDFS_ROOT=/user/${USER}/tpcds

# scale factor in GB
# SF 3000 yields ~1TB for the store_sales table
export TPCDS_SCALE_FACTOR=3000

# this is used to determine the number of dsdgen processes to generate data
# usually set to one per physical CPU core
# example - 20 nodes @ 12 threads each
export DSDGEN_NODES=20
export DSDGEN_THREADS_PER_NODE=12
export DSDGEN_TOTAL_THREADS=$((DSDGEN_NODES * DSDGEN_THREADS_PER_NODE))

# the name for the tpcds database
export TPCDS_DBNAME=tpcds_parquet

# this is used to set additional options for impala-shell
# -k : Enable kerberos authentication
# -i <hostname> : Specify the impalad host, useful if you're using a load balancer
export IMPALA_SHELL_OPTS=""

# Set the REQUEST_POOL to use for Impala queries
export REQUEST_POOL="default"

# hostname of an impalad instance to connect to from master node
IMPALAD=""
