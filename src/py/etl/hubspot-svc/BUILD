python_sources(name="lib")

python_awslambda(
    name="hubspot-update-company",
    runtime="python3.8",
    handler="handler:handle",
    description="Batch update company via S3 trigger",
    dependencies=["3rdparty/py:boto3", "3rdparty/py:hubspot-api-client"],
)