import json
import logging
import os
import urllib.parse

import awswrangler as wr
import pandas as pd

rs_secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
rs_db_name = os.environ["REDSHIFT_DB"]
rs_dim_schema = os.environ["REDSHIFT_DIMENSION_SCHEMA"]
rs_fact_schema = os.environ["REDSHIFT_FACT_SCHEMA"]


def handle(event, context):
    # Extract S3 event from SNS message
    sns_body = event["Records"][0]["Sns"]["Message"]
    s3_event = json.loads(sns_body)

    # Extract S3 file URI
    bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        s3_event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    logging.info(f"s3 create event: s3://{bucket}/{key}")

    # Get the CDC file
    cdc_df = wr.s3.read_parquet(
        f"s3://{bucket}/{key}",
        columns=[
            "approximate_creation_date_time",
            "event_name",
            "company_id",
            "platform_name",
        ],
    )

    # Get all company ids
    company_ids = cdc_df["company_id"].drop_duplicates().to_list()

    # Get RedShift company dimension
    with wr.redshift.connect(secret_id=rs_secret_arn, dbname=rs_db_name) as conn:
        company_df = wr.redshift.read_sql_query(
            f"""
            SELECT company_key, dynamodb_key as company_id
            FROM {rs_dim_schema}.dim_company
            WHERE company_id IN {company_ids}
            """,
            con=conn,
        )

    # Get unregistered companies in RedShift
    new_company_df = pd.merge(cdc_df, company_df, how="left", on="company_id")
    new_company_df = new_company_df[new_company_df["company_key"].isna()]
    new_company_df = new_company_df[["company_id"]].rename(
        columns={"company_id": "dynamodb_key"}
    )

    # Update the dimension and refresh
    with wr.redshift.connect(secret_id=rs_secret_arn, dbname=rs_db_name) as conn:
        wr.redshift.to_sql(
            new_company_df,
            schema=rs_dim_schema,
            table="dim_company",
            mode="append",
            use_column_names=True,
            con=conn,
        )

        company_df = wr.redshift.read_sql_query(
            f"""
            SELECT company_key, dynamodb_key as company_id
            FROM {rs_dim_schema}.dim_company
            WHERE company_id IN {company_ids}
            """,
            con=conn,
        )

    fact_df = pd.merge(cdc_df, company_df, on="company_id", how="inner")

    # Create date key
    fact_df["year"] = fact_df["approximate_creation_date_time"].dt.year
    fact_df["month"] = fact_df["approximate_creation_date_time"].dt.year
    fact_df["day"] = fact_df["approximate_creation_date_time"].dt.day
    fact_df["date_key"] = pd.to_numeric(
        fact_df["year"].astype("str")
        + fact_df["month"].astype("str")
        + fact_df["day"].astype("str"),
        errors="coerce",
    )

    # Create time key
    fact_df["hour"] = fact_df["approximate_creation_date_time"].dt.hour
    fact_df["minute"] = fact_df["approximate_creation_date_time"].dt.minute
    fact_df["second"] = fact_df["approximate_creation_date_time"].dt.second
    fact_df["time_key"] = pd.to_numeric(
        fact_df["hour"].astype("str")
        + fact_df["minute"].astype("str").zfill(2)
        + fact_df["second"].astype("str").zfill(2),
        errors="coerce",
    )

    # Rename column
    fact_df = fact_df.rename(columns={"platform_name": "platform"})

    # Convert event to status
    fact_df["status"] = fact_df["event_name"].map({"INSERT": True, "REMOVE": False})

    # Drop invalid rows
    fact_df = fact_df.dropna()

    # Form columns
    fact_df = fact_df[["date_key", "time_key", "company_key", "platform", "status"]]

    with wr.redshift.connect(secret_id=rs_secret_arn, dbname=rs_db_name) as conn:
        wr.redshift.to_sql(
            new_company_df,
            schema=rs_fact_schema,
            table="fact_open_platform_connection",
            mode="append",
            use_column_names=True,
            con=conn,
        )

    return {
        "status": 200,
        "total": cdc_df.count(),
        "new_companies": new_company_df.count(),
        "new_fact": fact_df.count(),
    }
