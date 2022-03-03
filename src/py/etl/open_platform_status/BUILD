python_sources(name='lib')

python_awslambda(
    name='extract-open-platform',
    runtime='python3.8',
    handler='handlers/extract_open_platform:handle',
    description='Export open platform status to S3 raw bucket',
    dependencies=['3rdparty/py:awswrangler']
)

python_awslambda(
    name='clean-open-platform',
    runtime='python3.8',
    handler='handlers/clean_open_platform:handle',
    description='Clean exported data and load into S3 clean bucket',
)

python_awslambda(
    name='load-open-platform',
    runtime='python3.8',
    handler='handlers/load_open_platform:handle',
    description='Load cleaned data into RedShift',
)
