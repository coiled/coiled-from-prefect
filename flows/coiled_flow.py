import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import coiled
import dask.dataframe as dd
from dask.dataframe.utils import make_meta
from distributed import get_client
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.exceptions import FailedRun
from prefect_aws import AwsCredentials
from prefect_dask import DaskTaskRunner
from s3fs import S3FileSystem

os.environ["DASK_COILED__ACCOUNT"] = Secret.load("coiled-account").get()
os.environ["DASK_COILED__TOKEN"] = Secret.load("coiled-token").get()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@task
def load_and_clean_data(files_to_process, creds):
    """
    Retrieve AWS Credentials from a Prefect Block, do some basic cleaning,
    repartition the data and write it to a private S3 bucket
    """

    logger = get_run_logger()

    try:
        # Read the files into a Dask DataFrame & preprocess
        with get_client() as client:
            ddf = dd.read_parquet(files_to_process)
            logger.info("Loaded dataframe")
            ddf["airport_fee"] = ddf["airport_fee"].astype(str)
            ddf = ddf.repartition(partition_size="128MB")
            logger.info("Doing write.")

            # We provide a unique filename for each partition in place of Dask's
            # standard `part-i-` convention.  This is to avoid collisions during
            # incremental processing.
            name_func = (
                lambda x: f"fhvhv_tripdata_{str(uuid.uuid1(clock_seq=int(x)))}.parquet"
            )
            dd.to_parquet(
                ddf,
                "s3://prefect-dask-examples/nyc-taxi-uber-lyft/split_files.parquet",
                storage_options={
                    "key": creds.aws_access_key_id,
                    "secret": creds.aws_secret_access_key.get_secret_value(),
                    "client_kwargs": {"region_name": "us-east-2"},
                },
                name_function=name_func,
            )
        logger.info("Completed write operation")

    except Exception:
        raise FailedRun


@task
def log_summary(x):
    logger = get_run_logger()
    logger.info(x)


@flow(
    name="clean-files-from-s3",
    task_runner=DaskTaskRunner(
        cluster_class="coiled.Cluster",
        cluster_kwargs={
            "n_workers": 5,
            "account": Secret.load("coiled-team-account").get(),
            "name": f"nyc-taxi-uber-lyft-{str(uuid.uuid1())}",
            "backend_options": {"region": "us-east-1"},
            "worker_memory": "64 GiB",
            "scheduler_options": {"idle_timeout": "0.5 hours"},
            "package_sync": True,
        },
    ),
)
def clean_data(files_to_process):
    logger = get_run_logger()
    logger.info("Clean data")
    creds = AwsCredentials.load("prefectexample0")
    prefect_future = load_and_clean_data.submit(files_to_process, creds)
    return prefect_future.result()


@flow(name="Check for files")
def check_for_files(intent: str):
    """
    We're going to run this on a daily schedule.  To find files to process incrementally
    we get files from the S3 bucket that match `fhvhv_tripdata_*.parquet` and evaluate
    look at their modification timestamp.  We only want to process files that were
    modified in the last day.

    Parameters
    ----------

    intent: str: one of subset, or incremental
    """
    logger = get_run_logger()
    if intent == "reprocess":
        files = "s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet"

    else:
        fs = S3FileSystem()
        last_week = datetime.now(timezone.utc) - timedelta(days=7)
        files = fs.glob("s3://nyc-tlc/trip data/fhvhv_tripdata_*.parquet", detail=True)
        if intent == "test_subset":
            files = [v["Key"] for _, v in files.items()]
            files = files[0]
            files = [files]
        else:
            files = [
                v["Key"] for _, v in files.items() if v["LastModified"] > last_week
            ]

        # Add the protocol back to the filename
        files = [f"s3://{f}" for f in files]
        logger.info(f"Found {len(files)} files that are:  {files}")
    if files:
        clean_data(files)


if __name__ == "__main__":
    check_for_files(intent="test_subset")
