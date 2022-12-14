import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import coiled
import dask.dataframe as dd
import pyarrow as pa
from dask.dataframe.utils import make_meta
from distributed import Client
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from prefect_aws.secrets_manager import read_secret
from s3fs import S3FileSystem

os.environ["DASK_COILED__ACCOUNT"] = Secret.load("coiled-account").get()
os.environ["DASK_COILED__TOKEN"] = Secret.load("coiled-token").get()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@task
def check_for_new_files():
    """
    We're going to run this on a daily schedule.  To find files to process incrementally
    we get files from the S3 bucket that match `fhvhv_tripdata_*.parquet` and evaluate
    look at their modification timestamp.  We only want to process files that were
    modified in the last day.
    """
    fs = S3FileSystem(anon=True)
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    files = fs.glob("s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet", detail=True)
    files = [k for k, v in files.items() if v["LastModified"] > yesterday]

    # Add back the protocol information
    return [f"s3://{f}" for f in files]


@task
def load_data_from_s3(files_to_load):
    logger.info("Fetching client")
    if isinstance(files_to_load, str):
        return dd.read_parquet(files_to_load)
    else:
        return dd.read_


@task
def repartition_and_write_ddf(ddf):
    """
    Retrieve AWS Credentials from a Prefect Block, do some basic cleaning,
    repartition the data and write it to a private S3 bucket
    """
    creds = AwsCredentials.load("prefectexample0")
    ddf = ddf.fillna(0)
    df = ddf.head()
    schema = pa.Schema.from_pandas(df)

    ddf = ddf.repartition(partition_size="128MB")

    name_func = lambda x: f"fhvhv_tripdata_{str(uuid.uuid1(clock_seq=int(x)))}.parquet"
    dd.to_parquet(
        ddf,
        "s3://prefect-dask-examples/test0/files.parquet",
        storage_options={
            "key": creds.aws_access_key_id,
            "secret": creds.aws_secret_access_key.get_secret_value(),
            "client_kwargs": {"region_name": "us-east-2"},
        },
        schema=schema,
    )


@flow(name="clean-files-from-s3", retries=3)
def clean_data(reprocess: bool = True):
    if reprocess is True:
        ddf = load_data_from_s3("s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet")
    else:
        files_to_process = check_for_new_files()
        if files_to_process:
            ddf = load_data_from_s3(files_to_process)
        else:
            return
    repartition_and_write_ddf(ddf)


if __name__ == "__main__":
    logger.info("Start dask cluster on Coiled")

    with coiled.Cluster(
        n_workers=10,
        account=Secret.load("coiled-team-account").get(),
        name=f"nyc-taxi-uber-lyft-{str(uuid.uuid1())}",
        backend_options={"region": "us-east-1"},
        worker_memory="64 GiB",
        scheduler_options={"idle_timeout": "0.5 hours"},
        software="coiled/coiled-runtime-0-2-0-py39",
    ) as cluster:
        client = Client(cluster)
        clean_data(reprocess=True)
