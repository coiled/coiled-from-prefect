## Run A Data Engineering Workload on Coiled From Prefect

This repo provides a starting point for automating a Dask engineering workload using Coiled and Prefect

### In this workload, we create a Prefect Flow that performs the following actions on once daily:
1.  Creates a Dask cluster from Coiled 
2.  Grabs a list of parquet files from s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet  
3. Checks to see if any of those files were written in the last day, using the `LastModified` time as a proxy for when the file was written.  This allows incremental processing of the files
4. If any files meet this criteria, they are passed to the Dask cluster for processing ane repartitioning, then written to a different private S3 bucket
5. Tears down the Dask cluster



### Getting Started
This repo assumed you have a Coiled account, a Prefect account, are familiar with the following concepts, and have access to the following access credentials:

- Coiled Accounts, Teams, and Tokens 
- Prefect Cloud
- Prefect Flows and Tasks
- Prefect Blocks
- AWS S3 Access Key and Secret


1.  Clone this repository
2.  In Prefect, store your credentials to create a `Coiled` cluster as `Prefect Blocks`.  Here we store them as:  `coiled-account`, `coiled-team-account`, and `coiled-token`.
3.  In Prefect, store your credentials to authenticate to your private S3 bucket as a `Prefect AwsCredentials`.  Here, we label it `prefectexample0`



#### References  
[Coiled](https://www.coiled.io)  
[Dask](https://www.dask.org)  
[Prefect](https://www.prefect.io/)
[Working with Dask collections](https://docs.coiled.io/user_guide/examples/prefect-v2.html#working-with-dask-collections)  
[Prefect-Dask](https://prefecthq.github.io/prefect-dask/#distributing-dask-collections-across-workers)   
[Workflow Automation with Prefect](https://docs.coiled.io/user_guide/examples/prefect-v2.html#computing-dask-collections)  
[PrefectHTTPStatus Errors](https://discourse.prefect.io/t/how-can-i-resolve-this-error-prefecthttpstatuserror-on-prefect-2-3-1-dasktaskrunner/1541/5)  

