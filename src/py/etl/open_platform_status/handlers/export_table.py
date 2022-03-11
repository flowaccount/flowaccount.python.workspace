import os
import uuid
from datetime import datetime

import boto3

client = boto3.client("dynamodb")
dynamodb_arn = os.environ["DYNAMODB_ARN"]
raw_bucket = os.environ["RAW_BUCKET"]
s3_prefix = os.environ["S3_PREFIX"]


def handle(event, context):
    """Start DynamoDB Export to S3 job."""

    table_name = event["table"]
    table_arn = f"{dynamodb_arn}:table/{table_name}"

    if "export_time" in event:
        export_time = datetime.fromisoformat(event["export_time"])
    else:
        export_time = datetime.now()

    if "client_token" in event:
        client_token = event["client_token"]
    else:
        client_token = None

    result = client.export_table_to_point_in_time(
        TableArn=table_arn,
        ExportTime=export_time,
        ClientToken=client_token,
        S3Bucket=raw_bucket,
        S3Prefix=f"{s3_prefix}/{table_name}",
        ExportFormat="DYNAMODB_JSON",
    )

    response = {
        "statusCode": 200,
        "export": result,
    }

    return response
