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

### Clone the Vertica TPC-DS tools repo & Configure the HDFS directories

* `cd $HOME`
* clone this repo `git clone https://github.com/sohan/vertica-tpcds-kit`
* `cd vertica-tpcds-kit`
* `cp tpcds-env-sample.sh tpcds-env.sh` and modify tpcds-env.sh as needed.
* Edit `dn.txt` and put one DataNode hostname per line - no blank lines.
* Run `push-bits.sh` which will scp `vertica-tpcds-kit` and build `tpcds-kit` on each DataNode listed in `dn.txt`.
* Run `set-nodenum.sh`.  This will create `vertica-tpcds-kit/nodenum.sh` on every DataNode and set the value accordingly. This is used to determine what portion of the distributed data generation is done on each node.

## Preparation and Data Generation

Data is landed directly in Vertica so there is no requirement for any local storage. Run the following in order:

* `vertica-create-tables.sh` - Creates tables in Vertica
* `run-gen-dims.sh` - Runs `gen-dims.sh` on the first node in dn.txt. Generate dimension data and insert into Vertica
* `run-gen-facts.sh` - Runs `gen-facts.sh` on each DataNode via ssh to generate the `store_sales` table.

## Run Queries and record times

You can run queries in a single-user or multi-user (concurrent) setting. See below for usage.

```
./vertica-run-queries.sh --help
./vertica-run-queries.sh --interactive --num-users 5 --num-runs 3
```

```
cat results/*-scale-5-users-3-runs/metadata.json

{
    "metadata": {
        "num_users": 5, 
        "num_runs": 3, 
        "start_time": "2014-09-04 15:50:38.380715", 
        "scale_factor": "10", 
        "queries": [
            "q19", 
            ...
        ]
    }
}
```

```
cat results/*-scale-5-users-3-runs/query_times.csv

"run_number","user","query","time_s"
"1","1","q42","0.17443999999999998"
"1","1","q52","0.432267"
"1","2","q19","0.763254"
"1","3","q52","0.467401"
"1","2","q42","0.509011"
"1","3","q42","0.760072"
"1","1","q19","1.065039"
"1","3","q73","0.469087"
"1","4","q68","1.527678"
"1","1","q98","1.8634000000000002"
"1","4","q98","2.42102"
"1","5","q98","2.111182"
...

```

