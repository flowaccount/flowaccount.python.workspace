import os
from datetime import date, time
from typing import Tuple

import awswrangler as wr
import pandas as pd
from redshift_connector import Connection as RedShiftConnection

clean_bucket = os.environ["CLEAN_BUCKET"]
table_key = os.environ["TABLE_KEY"]
secret_id = os.environ["REDSHIFT_SECRET_ARN"]
schema = os.environ["REDSHIFT_SCHEMA"]


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


def get_company_from_redshift(schema: str, conn: RedShiftConnection) -> pd.DataFrame:
    """Get company dimension from RedShift."""
    redshift_df = wr.redshift.read_sql_query(
        f"SELECT company_key, dynamodb_key FROM {schema}.dim_company", con=conn
    )

    return redshift_df


def get_platform_from_redshift(schema: str, conn: RedShiftConnection) -> pd.DataFrame:
    """Get latest (company, platform) pair connection status from RedShift."""

    redshift_df = wr.redshift.read_sql_query(
        f"""
            WITH cte_1 AS (
                SELECT
                    company_key,
                    platform,
                    status,
                    ROW_NUMBER() OVER(
                        PARTITION BY company_key, platform
                        ORDER BY date_key DESC
                    ) AS row_num
                FROM {schema}.fact_open_platform_connection
            )
            SELECT
                f.company_key AS company_key,
                c.dynamodb_key AS dynamodb_key,
                f.platform AS platform,
                f.status AS status
            FROM cte_1 AS f
            JOIN {schema}.dim_company AS c ON c.company_key = f.company_key
            WHERE row_num = 1
        """,
        con=conn,
    )

    redshift_df["platform"] = redshift_df["platform"].astype("category")

    return redshift_df


def get_disconnected(fact_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    """Get disconnected (company, platform) pairs by finding pairs which exist
    only in RedShift."""

    df = fact_df.join(incoming_df, how="left", on="company_key", rsuffix=".incoming")
    df = df[df["company_key.incoming"].isna()]
    df = df[["company_key", "platform"]]

    return df


def get_connected(fact_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    """Get connected (company, platform) pairs by using incoming pairs."""

    df = incoming_df[["company_key", "platform_name"]].rename(
        columns={"platform_name": "platform"}
    )
    return df


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

    with wr.redshift.connect(secret_id=secret_id, dbname="test") as conn:
        rs_platform_df = get_platform_from_redshift(schema, conn)

        # Attach company dimension to S3 dataframe
        rs_company_df = get_company_from_redshift(schema, conn)
        s3_platform_df = s3_platform_df.merge(
            rs_company_df, how="inner", left_on="company_id", right_on="dynamodb_key"
        )

        # Get disconnected
        disconnected_df = get_disconnected(rs_platform_df, s3_platform_df)
        disconnected_df["date_key"] = date_key
        disconnected_df["time_key"] = time_key
        disconnected_df["status"] = False

        # Get connected
        connected_df = get_connected(rs_platform_df, s3_platform_df)
        connected_df["date_key"] = date_key
        connected_df["time_key"] = time_key
        connected_df["status"] = True

        # Combine both dataframes
        df = pd.concat([disconnected_df, connected_df])

        # Write to RedShift
        wr.redshift.to_sql(
            df=df,
            table="fact_open_platform_connection",
            schema=schema,
            con=conn,
            mode="append",
            use_column_names=True,
        )

    response = {"statusCode": 200}
    return response
