import json
import urllib.parse
from typing import Dict, List, Tuple

import awswrangler as wr
import boto3
import pandas as pd
from flowaccount.utils import format_snake_case

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
    df = df.rename(columns=format_snake_case)
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
    df["table"] = df["table_arn"].str.rsplit(pat="/", n=1, expand=True)[1]

    return df


def clean_manifest_files(manifiest_files: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(manifiest_files)
    df = df.rename(columns=format_snake_case)
    df = df.astype(
        {
            "item_count": "int",
            "md5_checksum": "string",
            "etag": "string",
            "data_file_s3_key": "string",
        }
    )

    return df


def clean_exported_files(files_df: pd.DataFrame) -> pd.DataFrame:
    def format_column_name(col: str) -> str:
        name = col.rsplit(".", maxsplit=1)[0]
        return format_snake_case(name)

    df_list = [
        wr.s3.select_query(
            sql="SELECT * FROM s3object[*]",
            path=f"s3://{bucket}/{key}",
            input_serialization="JSON",
            input_serialization_params={"Type": "Document"},
            compression="gzip",
        )
        for bucket, key in zip(files_df["bucket"], files_df["data_file_s3_key"])
    ]

    df = pd.concat(df_list)
    df = pd.json_normalize(df["Item"])

    # Rename columns e.g. companyId.N --> company_id
    df = df.rename(columns=format_column_name)

    # Fill NaN with -1 for integer columns
    for col in [
        "company_id",
        "payment_channel_id",
        "expires_in",
        "refresh_expires_in",
        "user_id",
    ]:
        df[col] = df[col].fillna(-1)

    df = df.astype(
        {
            "company_id": "int64",
            "shop_id": "string",
            "platform_info": "string",
            "is_delete": "bool",
            # 'expired_at': 'int64',
            "payment_channel_id": "int64",
            # 'created_at': 'int64',
            "expires_in": "int64",
            "is_vat": "bool",
            "payload": "string",
            "guid": "string",
            "refresh_expires_in": "int64",
            "user_id": "int64",
            # 'updated_at': 'int64',
            "platform_name": "string",
            "refresh_token": "string",
            "remarks": "string",
            "access_token": "string",
            "email": "string",
        }
    )

    # Convert datetime string to datetime64
    df["expired_at"] = pd.to_datetime(df["expired_at"], unit="s")
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s")

    # NOTE: Pandas cannot write timedelta to parquet files. It needs 'fastparquet' engine.
    # records['expires_in'] = pd.to_timedelta(records['expires_in'], unit='s')
    # records['refresh_expires_in'] = pd.to_timedelta(records['refresh_expires_in'], unit='s')

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
    print("Clean manifest summary")
    summary_df = clean_manifest_summary(manifest_summary)

    # Common attributes
    export_id = summary_df["export_id"][0]
    table = summary_df["table_arn"][0].rsplit("/")[1]
    year = summary_df["year"][0]
    month = summary_df["month"][0]

    # Clean manifest files
    print("Clean manifest files")
    files_df = clean_manifest_files(manifest_files)
    files_df["export_id"] = export_id
    files_df["bucket"] = bucket
    files_df["year"] = year
    files_df["month"] = month

    # Clean exported open platform table
    print("Clean table")
    table_df = clean_exported_files(files_df)
    table_df["export_id"] = export_id
    table_df["year"] = year
    table_df["month"] = month

    # Create Glue database catalog if not exists
    databases = wr.catalog.databases()
    if clean_catalog not in databases.values:
        wr.catalog.create_database(clean_catalog)

    # Write manifest summary
    print(f"Write cleaned manifest summary: {export_id}")
    cleaned_s3_summary = wr.s3.to_parquet(
        df=summary_df,
        path=f"s3://{clean_bucket}/dynamodb/exports/summary",
        dataset=True,
        partition_cols=["table", "year", "month"],
        database=clean_catalog,
        table="dynamodb_export_manifest_summary",
    )

    # Write manifest files
    print("Write cleaned manifest files")
    cleaned_s3_files = wr.s3.to_parquet(
        df=files_df,
        path=f"s3://{clean_bucket}/dynamodb/exports/files",
        dataset=True,
        partition_cols=["year", "month"],
        database=clean_catalog,
        table="dynamodb_export_manifest_files",
    )

    # Write table records
    print(f"Write cleaned table: {table}")
    cleaned_s3_table = wr.s3.to_parquet(
        df=table_df,
        path=f"s3://{clean_bucket}/dynamodb/tables/{table}",
        dataset=True,
        partition_cols=["year", "month"],
        database=clean_catalog,
        table=f"dynamodb_{table}",
    )

    return {
        "statusCode": 200,
        "results": {
            "manifest_summary": cleaned_s3_summary["paths"],
            "manifest_files": cleaned_s3_files["paths"],
            "table": cleaned_s3_table["paths"],
        },
    }
