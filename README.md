# A TPC-DS like benchmark for Cloudera Impala


**NOTICE: This repo contains modifications to the official TPC-DS specification so any results from this are not comparable to officially audited results.**

## Schema Information

The following tables are currently used:

Dimension Tables:

* DATE_DIM
* TIME_DIM
* CUSTOMER
* CUSTOMER_ADDRESS
* CUSTOMER_DEMOGRAPHICS
* HOUSEHOLD_DEMOGRAPHICS
* ITEM
* PROMOTION
* STORE

Fact Tables:

* STORE_SALES

## Environment Setup Steps

These steps setup your environment to perform a distributed data generation for the given
scale factor.

### Prerequisites

The scripts assume that you have passwordless SSH from the master node (where you will clone the repos to) to every DataNode that is in your cluster.

These scripts also assume that your $HOME directory is the same path on all DataNodes.

### Download and build the modified TPC-DS tools

* `sudo yum -y install gcc make flex bison byacc git`
* `cd $HOME` (use your `$HOME` directory as it's hard coded in some scripts for now)
* `git clone https://github.com/gregrahn/tpcds-kit.git`
* `cd tpcds-kit`
* `git checkout --quiet eff5de2`
* `cd tools`
* `make -f Makefile.suite`

### Clone the Impala TPC-DS tools repo & Configure the HDFS directories

* `cd $HOME` (use your `$HOME` directory as it's hard coded in some scripts for now)
* clone this repo `git clone https://github.com/cloudera/impala-tpcds-kit`
* `cd impala-tpcds-kit`
* Edit `tpcds-env.sh` and modify as needed.  The defaults assume you have a `/user/$USER` directory in HDFS.  If you don't, run these commands:
  * `sudo -u hdfs hdfs dfs -mkdir /user/$USER`
  * `sudo -u hdfs hdfs dfs -chown $USER /user/$USER`
  * `sudo -u hdfs hdfs dfs -chmod 777 /user/$USER`
* Edit `dn.txt` and put one DataNode hostname per line - no blank lines.
* Run `push-bits.sh` which will scp `tpcds-kit` and `impala-tpcds-kit` to each DataNode listed in `dn.txt`.
* Run `set-nodenum.sh`.  This will create `impala-tpcds-kit/nodenum.sh` on every DataNode and set the value accordingly.  This is used to determine what portion of the distributed data generation is done on each node.

## Preparation and Data Generation

Data is landed directly in HDFS so there is no requirement for any local storage.

* `hdfs-mkdirs.sh` - Make HDFS directories for each table.
* `gen-dims.sh` - Generate dimension flat files (runs on one DataNode only).
* `run-gen-facts.sh` - Runs `gen-facts.sh` on each DataNode via ssh to generate STORE_SALES flat files.

## Data Loading

### Impala Steps
* `impala-create-external-tables.sh` - Creates a Hive database and the external tables pointing to flat files.
* `impala-load-dims.sh` - Load dimension tables (no format specified, modify as necessary, but not required).
* `impala-load-store_sales.sh` - Load STORE_SALES table which uses dynamic partitioning, one partition per calendar day.
* `impala-compute-stats.sh` - Gather table and column statistics on all tables.

## Queries

`impala-tpcds-kit/queries` contains queries execute on Impala (v2.3+). Note that the
queries are not qualified with a database name. In order to run them, the impala-shell
needs to be run with the -d paramater. Alternatively, one can also issue a use db_name
before running each individual query.
