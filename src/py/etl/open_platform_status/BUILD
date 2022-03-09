python_sources(name="lib")

python_awslambda(
    name="export-table",
    runtime="python3.8",
    handler="handlers/export_table:handle",
    description="Export DynamoDB to S3",
    dependencies=["3rdparty/py:boto3"],
)

python_awslambda(
    name="extract-open-platform",
    runtime="python3.8",
    handler="handlers/extract_open_platform:handle",
    description="Export open platform status to S3 raw bucket",
    dependencies=["3rdparty/py:awswrangler"],
)

python_awslambda(
    name="clean-open-platform",
    runtime="python3.8",
    handler="handlers/clean_manifest:handle",
    description="Clean export manifest summary and manifest files",
    dependencies=["3rdparty/py:awswrangler"],
)

python_awslambda(
    name="load-company",
    runtime="python3.8",
    handler="handlers/load_company:handle",
    description="Load new companies into RedShift",
    dependencies=["3rdparty/py:awswrangler"],
)

python_awslambda(
    name="load-open-platform",
    runtime="python3.8",
    handler="handlers/load_open_platform:handle",
    description="Load cleaned data into RedShift",
    dependencies=["3rdparty/py:awswrangler"],
)
