## Run A Data Engineering Workload on Coiled From Prefect

This repo provides a starting point for automating a Dask engineering workload using Coiled and Prefect

### Motivating Problem

New York City recently started open-sourced Uber-Lyft data at: `s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet`

This is a robust dataset for exploration.  Unfortunately, while it is provided as a parquet dataset, the row groups are quite large, which make processing very unwieldly.  Additionally, datatypes between partitions can lead to unexpected errors.  We can leverage this as a motivating example for creating a pure Python data engineering pipeline using Coiled, Dask, and Prefect.

## In this workload, we create two Prefect Flows that perform the following actions once daily:

#### Flow 1
1.  Selects a list of parquet files from s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet  
2. Checks to see if any of those files were written in the last day, using the `LastModified` time as a proxy for when the file was written.  This allows incremental processing of the files
3. If any files meet this criteria, we kick off Flow 2

#### Flow 2
1.  Spin up a Dask cluster with Coiled
2.  Pass the list of files to be processed to the Dask cluster.  The files will be loaded into the cluster, we declare datatypes, repartition the files to `128MB` partitions, and write these files to a private S3 bucket.
3. Tears down the Dask cluster



### Getting Started
This repo assumed you have a Coiled account, a Prefect account, are familiar with the following concepts, and have access to the following access credentials:

- Coiled Users, Teams, and Tokens 
- Prefect Cloud
- Prefect Flows and Tasks
- Prefect Blocks
- AWS S3 Access Key and Secret


1.  Clone this repository
2.  In Prefect, store your credentials to create a `Coiled` cluster as `Prefect Blocks`.  Here we store them as:  `coiled-user`, `coiled-account` (your team), and `coiled-token`.
3.  In Prefect, store your credentials to authenticate to your private S3 bucket as a `Prefect AwsCredentials`.  Here, we label it `prefectexample0`



#### References  
[Coiled](https://www.coiled.io)  
[Dask](https://www.dask.org)  
[Prefect](https://www.prefect.io/)
[Working with Dask collections](https://docs.coiled.io/user_guide/examples/prefect-v2.html#working-with-dask-collections)  
[Prefect-Dask](https://prefecthq.github.io/prefect-dask/#distributing-dask-collections-across-workers)   
[Workflow Automation with Prefect](https://docs.coiled.io/user_guide/examples/prefect-v2.html#computing-dask-collections)  
[PrefectHTTPStatus Errors](https://discourse.prefect.io/t/how-can-i-resolve-this-error-prefecthttpstatuserror-on-prefect-2-3-1-dasktaskrunner/1541/5)  

