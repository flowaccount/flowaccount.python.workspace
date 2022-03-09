import json
import urllib.parse

import boto3

s3 = boto3.client("s3")


def handle(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    manifest_summary_key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    if manifest_summary_key.rsplit("/")[1] != "manifest-summary.json":
        return {"status": 400, "error": "Invalid manifest summary key"}

    manifest_summary = (
        s3.get_object(Bucket=bucket, key=manifest_summary_key)["Body"]
        .read()
        .decode("utf-8")
    )
    export_id = manifest_summary["exportArn"].rsplit("/")[1]
    table_name = manifest_summary["tableArn"].rsplit("/")[1]

    manifest_files_key = manifest_summary["manifestFilesS3Key"]
    manifest_files_raw = (
        s3.get_object(Bucket=bucket, key=manifest_files_key)["Body"]
        .read()
        .decode("utf-8")
    )
    manifest_files = [json.loads(line) for line in manifest_files_raw.splitlines()]

    response = {
        "status": 200,
        "export_id": export_id,
        "table_name": table_name,
        "bucket": bucket,
        "manifest_summary_key": manifest_summary_key,
        "manifest_files_key": manifest_files_key,
        "export_file_keys": [manifest["dataFileS3Key"] for manifest in manifest_files],
    }

    return response
