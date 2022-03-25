import json
import logging
import os
import urllib.parse

import awswrangler as wr
from flowaccount.etl.open_platform_status.load_redshift_streaming import (
    convert_to_fact_table, filter_new_company)

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
            WHERE company_id IN ({str(company_ids)[1:-1]})
            """,
            con=conn,
        )

    # Get unregistered companies in RedShift
    new_company_df = filter_new_company(cdc_df, company_df)
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

    fact_df = convert_to_fact_table(cdc_df, company_df)

    if fact_df.count() > 0:
        with wr.redshift.connect(secret_id=rs_secret_arn, dbname=rs_db_name) as conn:
            wr.redshift.to_sql(
                fact_df,
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
