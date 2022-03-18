import os
from pathlib import Path
import urllib.parse
from unittest import result
import boto3

s3 = boto3.client("s3")

src_prefix = os.environ("SRC_PREFIX")
dst_aws_id = os.environ("DST_AWS_ID")
dst_bucket = os.environ("DST_BUCKET")
dst_prefix = os.environ("DST_PREFIX")


def handle(event, context):
    src_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    src_key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    path = Path(src_key)

    # Extract partitions and filename from key
    try:
        path_after_prefix = path.relative_to(src_prefix)
        partition_cols = path_after_prefix.parent
        filename = path_after_prefix.name
    except ValueError:
        return {"statusCode": 400, "error": f"Invalid source key {src_key}"}
    
    # Construct destination key
    dst_key = str(Path(dst_prefix) / partition_cols / filename)

    # Copy file
    result = s3.copy_object(
        Bucket=dst_bucket,
        Key=f"s3://{dst_bucket}/${dst_key}/",
        CopySource={"Bucket": src_bucket, "Key": src_key},
        ExpectedBucketOwner=dst_aws_id
    )

    return {
        "status": 200,
        "destination_bucket": dst_bucket,
        "destination_key": dst_key,
        "etag": result["CopyObjectResult"]["ETag"],
        "checksum_sha256": result["CopyObjectResult"]["ChecksumSHA256"]
    }