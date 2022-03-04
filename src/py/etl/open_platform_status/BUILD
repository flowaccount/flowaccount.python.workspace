python_sources(name='lib')

python_awslambda(
    name='extract-open-platform',
    runtime='python3.8',
    handler='handlers/extract_open_platform:handle',
    description='Export open platform status to S3 raw bucket',
    dependencies=['3rdparty/py:awswrangler']
)

python_awslambda(
    name='clean-manifest',
    runtime='python3.8',
    handler='handlers/clean_manifest:handle',
    description='Clean export manifest summary and manifest files',
    dependencies=['3rdparty/py:awswrangler']
)

python_awslambda(
    name='load-open-platform',
    runtime='python3.8',
    handler='handlers/load_open_platform:handle',
    description='Load cleaned data into RedShift',
)
