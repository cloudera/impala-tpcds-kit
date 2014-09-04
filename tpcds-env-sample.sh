export VERTICA_HOST="10.0.0.1"
export VERTICA_DB="dw"
export VERTICA_USER="dbadmin"
export VERTICA_PW="password"
export VERTICA_PORT="5433"

export HOMEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
export CLUSTER_USER="root"
export CLUSTER_HOMEDIR="/home/root/"
export MPP_TPCDS_DIR="vertica-tpcds-kit"
export TPCDS_DIR="tpcds-kit"

# scale factor in GB
# SF 3000 yields ~1TB for the store_sales table
export TPCDS_SCALE_FACTOR=3000

# this is used to determine the number of dsdgen processes to generate data
# usually set to one per physical CPU core
# example - 20 nodes @ 12 threads each
export DSDGEN_NODES=20
export DSDGEN_THREADS_PER_NODE=12
export DSDGEN_TOTAL_THREADS=$((DSDGEN_NODES * DSDGEN_THREADS_PER_NODE))
