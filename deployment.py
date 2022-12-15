from prefect.filesystems import S3


from flows import coiled_flow
from prefect.deployments import Deployment

s3_block = S3.load("prefect-s3-storage")
deployment = Deployment.build_from_flow(
    flow=coiled_flow.check_for_files(intent="test_subset"),
    name="example",
    version="1",
    tags=["demo"],
    # storage=s3_block,
    # apply=True,
)
deployment.apply()
