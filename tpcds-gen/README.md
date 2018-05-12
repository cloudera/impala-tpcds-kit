MapReduce TPC-DS Generator
==========================

This simplifies creating TPC-DS datasets at large scales on a Hadoop cluster.

To get set up, you need to run

```
$ make
```

This will download the TPC-DS dsgen program, compile it and use maven to build the MR app wrapped around it.

To generate the data use a variation of the following command to specify the target directory in HDFS (`-d`), the scale factor in GB (`-s 10000`, for 10TB), and the parallelism to use (`-p 100`).

```
$ hadoop jar target/tpcds-gen-1.0-SNAPSHOT.jar -d /tmp/tpc-ds/sf10000/ -p 100 -s 10000
```

This uses the existing parallelism in the driver.c of TPC-DS without modification and uses it to run the command on multiple machines instead of running in local fork mode.

The command generates multiple files for each map task, resulting in each table having its own subdirectory.

Assumptions made are that all machines in the cluster are OS/arch/lib identical.
