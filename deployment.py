from flows import coiled_flow
from prefect.deployments import Deployment
deployment = Deployment.build_from_flow(
    flow=coiled_flow,
    name="example",
    version="1",
    tags=["demo"],
)
deployment.apply()
