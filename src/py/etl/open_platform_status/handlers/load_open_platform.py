import os
from datetime import date, time
from typing import Tuple

import awswrangler as wr
import pandas as pd
from redshift_connector import Connection as RedShiftConnection

clean_bucket = os.environ["CLEAN_BUCKET"]
table_key = os.environ["TABLE_KEY"]
secret_id = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]
dim_schema = os.environ["REDSHIFT_DIMENSION_SCHEMA"]
fact_schema = os.environ["REDSHIFT_FACT_SCHEMA"]


def format_date_key(date_obj: date) -> int:
    year, month, day = date_obj.year, date_obj.month, date_obj.day
    return int(f"{year}{month:02}{day:02}")


def format_time_key(time_obj: time) -> int:
    hour, minute, second = time_obj.hour, time_obj.minute, time_obj.second
    return int(f"{hour}{minute:02}{second:02}")


def get_export_datetime(s3_key: str) -> Tuple[date, time]:
    summary_df = wr.s3.read_parquet(s3_key)
    export_date = summary_df["export_time"].dt.date.iloc[0]
    export_time = summary_df["export_time"].dt.time.iloc[0]
    return export_date, export_time


def get_open_platform_from_s3(s3_key: str) -> pd.DataFrame:
    s3_df = wr.s3.read_parquet(s3_key)
    s3_df = s3_df[["company_id", "platform_name"]]
    s3_df["platform_name"] = s3_df["platform_name"].astype("category")
    return s3_df.reset_index(drop=True)


def get_platform_from_redshift(schema: str, conn: RedShiftConnection) -> pd.Series:
    """Get all known platforms in RedShift."""

    redshift_df = wr.redshift.read_sql_query(
        f"SELECT DISTINCT platform FROM {schema}.fact_open_platform_connection",
        con=conn,
    )

    return redshift_df["platform"]


def get_company_from_redshift(schema: str, conn: RedShiftConnection) -> pd.DataFrame:
    """Get company dimension from RedShift."""
    redshift_df = wr.redshift.read_sql_query(
        f"SELECT company_key, dynamodb_key FROM {schema}.dim_company", con=conn
    )

    return redshift_df


def get_platform_status(
    company_df: pd.DataFrame, s3_df: pd.DataFrame, platform_sr: pd.Series
) -> pd.DataFrame:
    """Get platform connection status of all companies."""

    # Pair each company with all platforms
    company_platform_df = company_df.merge(platform_sr, how="cross")

    # Merge each (company, platform) pair with S3 to see connection status
    merged_df = company_platform_df.merge(
        s3_df,
        how="left",
        left_on=["dynamodb_key", "platform"],
        right_on=["company_id", "platform_name"],
    )
    merged_df["status"] = merged_df["company_id"].isna()

    return merged_df[["company_key", "platform", "status"]]


def handle(event, context):
    export_id = event["export_id"]

    export_date, export_time = get_export_datetime(
        f"s3://{clean_bucket}/dynamodb/manifest/summary/{export_id}.parquet"
    )
    date_key = format_date_key(export_date)
    time_key = format_time_key(export_time)

    s3_platform_df = get_open_platform_from_s3(
        f"s3://{clean_bucket}/{table_key}/{export_id}.parquet"
    )

    with wr.redshift.connect(secret_id=secret_id, dbname=dbname) as conn:
        # Get all companies in RedShift
        rs_company_df = get_company_from_redshift(dim_schema, conn)

        # Get all known platforms from both S3 and RedShift
        rs_platform_sr = get_platform_from_redshift(fact_schema, conn)
        platform_sr = (
            pd.concat(
                rs_platform_sr, s3_platform_df["platform_name"], ignore_index=True
            )
            .rename("platform")
            .drop_duplicates()
        )

        platform_status_df = get_platform_status(
            rs_company_df, s3_platform_df, platform_sr
        )
        platform_status_df["date_key"] = date_key
        platform_status_df["time_key"] = time_key

        # Write to RedShift
        wr.redshift.to_sql(
            df=platform_status_df,
            table="fact_open_platform_connection",
            schema=fact_schema,
            con=conn,
            mode="append",
            use_column_names=True,
        )

    response = {"statusCode": 200}
    return response
