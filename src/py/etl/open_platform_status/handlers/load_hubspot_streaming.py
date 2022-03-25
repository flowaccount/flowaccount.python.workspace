import json
import logging
import os
import urllib.parse
import uuid

import awswrangler as wr
import boto3
from flowaccount.etl.open_platform_status.load_hubspot_streaming import (
    aggregate_latest_status, attach_hubspot_id, convert_to_json_line,
    convert_to_platform_status_dict, filter_event, filter_platform,
    get_hubspot_mapping)

rs_secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
rs_hubspot_schema = os.environ["REDSHIFT_HUBSPOT_SCHEMA"]
rs_hubspot_table = "company_ref"

hs_svc_bucket = os.environ["HUBSPOT_SVC_BUCKET"]
hs_svc_prefix = os.environ["HUBSPOT_SVC_COMPANY_UPDATE_PREFIX"]

s3 = boto3.client("s3")


def handle(event, context):
    # Extract S3 event from SNS message
    sns_body = event["Records"][0]["Sns"]["Message"]
    s3_event = json.loads(sns_body)

    # Extract S3 file URI
    bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        s3_event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(f"s3 create event: s3://{bucket}/{key}")

    cdc_df = wr.s3.read_parquet(
        f"s3://{bucket}/{key}", columns=["company_id", "event_name"]
    )

    with wr.redshift.connect(secret_id=rs_secret_arn, dbanme="test") as conn:
        companies = cdc_df["company_id"].drop_duplicates().to_list()
        rs_df = get_hubspot_mapping(
            companies, schema=rs_hubspot_schema, table=rs_hubspot_schema, conn=conn
        )

    # Attach HubSpot ID
    connection_df, missing_rel_df = attach_hubspot_id(cdc_df, rs_df)
    missing_rel = list(missing_rel_df["company_id"])
    logging.warning(f"Missing HubSpot ID: {missing_rel}")

    # Filter known platforms
    platform_df, missing_platform_df = filter_platform(connection_df)
    missing_platform = list(
        zip(missing_platform_df["company_id"], missing_platform_df["platform_name"])
    )
    logging.warning(f"Unknown platforms: {missing_platform}")

    # Filter event
    event_df, invalid_df = filter_event(platform_df)
    invalid_events = list(zip(invalid_df["company_id"], invalid_df["event_name"]))
    logging.warning(f"Ignore events: {invalid_events}")

    # Aggregate latest status for each company and platform pair
    agg_df = aggregate_latest_status(event_df)

    # Transform to HubSpot update input format
    inputs = [
        {
            "id": company_id,
            "properties": convert_to_platform_status_dict(agg_df.loc[company_id]),
        }
        for company_id in agg_df.index.drop_duplicates()
    ]

    # Export to HubSpot service bucket
    body = bytes(convert_to_json_line(inputs).encode("utf-8"))
    file_name = str(uuid.uuid4()) + ".json"
    export_key = f"{hs_svc_prefix}/{file_name}"
    s3.put_object(Bucket=hs_svc_bucket, Key=export_key, Body=body)

    return {
        "status": 200,
        "bucket": bucket,
        "key": key,
        "dst_bucket": hs_svc_bucket,
        "dst_key": export_key,
        "total": cdc_df.count(),
        "success": len(inputs),
        "failed": {
            "missing_hubspot_id": missing_rel_df.count(),
            "missing_platform": missing_platform_df.count(),
        },
    }
