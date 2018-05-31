# A TPC-DS like benchmark for Apache Impala

The official and latest TPC-DS tools and specification can be found at
[tpc.org](http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp)

## Step 0: Environment Setup

Install Java JDK and Maven if need be:

```
sudo yum -y install java-1.8.0-openjdk-devel maven
```

Install the necessary development tools:

```
sudo yum -y install git gcc make flex bison byacc curl unzip patch
```

## Step 1: Generate Data

Data generation is done via a MapReduce wrapper around TPC-DS `dsdgen`.  See `tpcds-gen/README.md` for more details on the commands to generate the flat files.

## Step 2: Load Data

Adjust the source/text and target/Parquet schema names and flat file paths in the sql files found in the `scripts/` directory.  See the comments at the top of each.

Create external text file tables:

```
impala-shell -f impala-external.sql
```

Create Parquet tables:

```
impala-shell -f impala-parquet.sql
```

Load Parquet tables and compute stats:

```
impala-shell -f impala-insert.sql
```

## Step 3: Run Queries

Sample queries from the 10TB scale factor can be found in the `queries/` directory.  The `query-templates/` directory contains the Apache Impala TPC-DS query templates which can be used with `dsqgen` (found in the official TPC-DS tools) to generate queries for other scale factors or to generate more queries with different substitution variables.
