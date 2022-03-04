import json
import urllib.parse
from typing import Dict, List, Tuple

import awswrangler as wr
import boto3
import pandas as pd

s3 = boto3.client("s3")
clean_bucket = "pipat-clean-bucket"
clean_catalog = "clean"


def get_manifest_from_event(
    bucket: str, summary_key: str
) -> Tuple[Dict[str, str], List[Dict[str, str]]]:

    summary_obj = s3.get_object(Bucket=bucket, Key=summary_key)
    manifest_summary = json.loads(summary_obj["Body"].read().decode("utf-8"))

    files_key = manifest_summary["manifestFilesS3Key"]
    files_obj = s3.get_object(Bucket=bucket, Key=files_key)
    files_str = files_obj["Body"].read().decode("utf-8")
    manifest_files = [json.loads(file_str) for file_str in files_str.splitlines()]

    return manifest_summary, manifest_files


def clean_manifest_summary(manifest_summary: dict) -> pd.DataFrame:
    df = pd.DataFrame([manifest_summary])
    df = df.rename(
        columns={
            "version": "version",
            "exportArn": "export_arn",
            "startTime": "start_time",
            "endTime": "end_time",
            "tableArn": "table_arn",
            "exportTime": "export_time",
            "s3Bucket": "s3_bucket",
            "s3Prefix": "s3_prefix",
            "s3SseAlgorithm": "s3_sse_algorithm",
            "s3SseKmsKeyId": "s3_sse_kms_key_id",
            "manifestFilesS3Key": "manifest_files_s3_key",
            "billedSizeBytes": "billed_size_bytes",
            "itemCount": "item_count",
            "outputFormat": "output_format",
        }
    )
    df = df.astype(
        {
            "version": "string",
            "export_arn": "string",
            "start_time": "datetime64",
            "end_time": "datetime64",
            "table_arn": "string",
            "export_time": "datetime64",
            "s3_bucket": "string",
            "s3_prefix": "string",
            "s3_sse_algorithm": "string",
            "s3_sse_kms_key_id": "string",
            "manifest_files_s3_key": "string",
            "billed_size_bytes": "int",
            "item_count": "int",
            "output_format": "string",
        }
    )

    # Convert datetime columns
    for col in ["start_time", "end_time", "export_time"]:
        df[col] = pd.to_datetime(df[col])

    df["year"] = df["export_time"].dt.year
    df["month"] = df["export_time"].dt.month

    df["export_id"] = df["export_arn"].str.rsplit(pat="/", n=1, expand=True)[1]

    return df


def clean_manifest_files(
    manifiest_files: List[dict], export_id: str, year: int, month: int
) -> pd.DataFrame:
    df = pd.DataFrame(manifiest_files)
    df = df.rename(
        columns={
            "itemCount": "item_count",
            "md5Checksum": "md5_checksum",
            "etag": "etag",
            "dataFileS3Key": "data_file_s3_key",
        }
    )
    df = df.astype(
        {
            "item_count": "int",
            "md5_checksum": "string",
            "etag": "string",
            "data_file_s3_key": "string",
        }
    )

    df["export_id"] = export_id
    df["year"] = year
    df["month"] = month

    return df


def handle(event, context):
    # Extract S3 file URI
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(f"s3 create event: s3://{bucket}/{key}")

    # Check S3 file firing the event
    if key.rsplit("/", maxsplit=1)[1] != "manifest-summary.json":
        return {"statusCode": 400, "error": "Invalid file trigger"}

    summary_key = key
    manifest_summary, manifest_files = get_manifest_from_event(bucket, summary_key)

    # Clean manifest summary
    summary_df = clean_manifest_summary(manifest_summary)

    # Clean manifest files
    export_id = summary_df["export_id"][0]
    year = summary_df["year"][0]
    month = summary_df["month"][0]
    files_df = clean_manifest_files(manifest_files, export_id, year, month)

    # Create Glue database catalog if not exists
    databases = wr.catalog.databases()
    if clean_catalog not in databases.values:
        wr.catalog.create_database(clean_catalog)

    # Write manifest summary
    wr.s3.to_parquet(
        df=summary_df,
        path=f"s3://{clean_bucket}/dynamodb/exports/summary",
        dataset=True,
        partition_cols=["year", "month"],
        database=clean_catalog,
        table="dynamodb_export_manifest_summary",
    )

    # Write manifest files
    wr.s3.to_parquet(
        df=files_df,
        path=f"s3://{clean_bucket}/dynamodb/exports/files",
        dataset=True,
        partition_cols=["year", "month"],
        database=clean_catalog,
        table="dynamodb_export_manifest_files",
    )

    return {"statusCode": 200}
