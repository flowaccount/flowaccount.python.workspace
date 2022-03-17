import json
import os
import urllib.parse

import awswrangler as wr
import boto3
from flowaccount.etl.lambdas.clean_open_platform import clean_open_platform_cdc

s3 = boto3.client("s3")
clean_bucket = os.environ["CLEAN_BUCKET"]
clean_catalog = os.environ["CLEAN_CATALOG"]


def handle(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    cdc_obj = s3.get_object(Bucket=bucket, Key=key)
    cdc_lines = cdc_obj["Body"].read().decode("utf-8").splitlines()
    cdc_list = [json.loads(line) for line in cdc_lines]

    clean_df = clean_open_platform_cdc(cdc_list)
    wr.s3.to_parquet(
        clean_df,
        path=f"s3://{clean_bucket}/dynamodb/streaming/tables/flowaccount-open-platform-company-user-v2",
        dataset=True,
        database=clean_catalog,
        table="dynamodb_streaming_flowaccount-open-platform-company-user-v2",
        partition_cols=["year", "month"],
    )

    response = {"statusCode": 200}
    return response
