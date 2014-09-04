# A TPC-DS like benchmark for Vertica

Based on TPC-DS v1.1.0 found at [http://www.tpc.org/tpcds/](http://www.tpc.org/tpcds/) and the [Cloudera Impala benchmark](https://github.com/cloudera/impala-tpcds-kit)

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

### Download and build the modified TPC-DS tools

* `sudo yum -y install gcc make flex bison byacc git`
* `cd $HOME`
* `git clone https://github.com/grahn/tpcds-kit.git`
* `cd tpcds-kit/tools`
* `make -f Makefile.suite`

### Clone the Vertica TPC-DS tools repo & Configure the HDFS directories

* `cd $HOME`
* clone this repo `git clone https://github.com/sohan/vertica-tpcds-kit`
* `cd vertica-tpcds-kit`
* `cp tpcds-env-sample.sh tpcds-env.sh` and modify as needed.
* Edit `dn.txt` and put one DataNode hostname per line - no blank lines.
* Run `push-bits.sh` which will scp `tpcds-kit` and `vertica-tpcds-kit` to each DataNode listed in `dn.txt`.
* Run `set-nodenum.sh`.  This will create `vertica-tpcds-kit/nodenum.sh` on every DataNode and set the value accordingly.  This is used to determine what portion of the distributed data generation is done on each node.

## Preparation and Data Generation

Data is landed directly in Vertica so there is no requirement for any local storage.

* `vertica-create-tables.sh` - Creates tables in Vertica
* `run-gen-dims.sh` - Runs `gen-dims.sh` on the first node in dn.txt. Generate dimension data and insert into Vertica
* `run-gen-facts.sh` - Runs `gen-facts.sh` on each DataNode via ssh to generate the `store_sales` table.

## Queries


TODO: 

`vertica-tpcds-kit/queries` contains queries execute on Impala (v1.2.3+). Note that the
queries are not qualified with a database name. In order to run them, the impala-shell
needs to be run with the -d paramater. Alternatively, one can also issue a use db_name
before running each individual query.
