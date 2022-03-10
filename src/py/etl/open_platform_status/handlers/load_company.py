import os
import awswrangler as wr
import pandas as pd
from redshift_connector import Connection as RedShiftConnection

clean_bucket = os.environ["CLEAN_BUCKET"]
table_key = os.environ["TABLE_KEY"]
secret_id = os.environ["REDSHIFT_SECRET_ARN"]
schema = os.environ["REDSHIFT_SCHEMA"]


def get_company_from_s3(s3_key: str):
    s3_df = wr.s3.read_parquet(s3_key)
    s3_df = s3_df[["company_id"]].drop_duplicates()
    return s3_df


def get_company_from_redshift(schema: str, conn: RedShiftConnection):
    redshift_df = wr.redshift.read_sql_query(
        f"SELECT dynamodb_key FROM {schema}.dim_company", con=conn
    )
    return redshift_df


def get_new_company(redshift_df: pd.DataFrame, s3_df: pd.DataFrame):
    """Find new companies in s3_df which are not in redshift_df."""

    # Discover new companies by find companies export table only has
    new_company_df = redshift_df.merge(
        s3_df, how="right", left_on="dynamodb_key", right_on="company_id"
    )
    new_company_df = new_company_df[new_company_df["dynamodb_key"].isna()]

    # Transform the DataFrame for insertion
    new_company_df = new_company_df[["company_id"]]
    new_company_df = new_company_df.rename(columns={"company_id": "dynamodb_key"})

    return new_company_df


def load_company(company_df: pd.DataFrame, schema: str, conn: RedShiftConnection):
    """Append RedShift company dimension with company_df."""

    wr.redshift.to_sql(
        df=company_df,
        table="dim_company",
        schema=schema,
        con=conn,
        mode="append",
        use_column_names=True,
    )


def handle(event, context):
    export_id = event["export_id"]

    # Get company ids from the export
    s3_df = get_company_from_s3(f"s3://{clean_bucket}/{table_key}/{export_id}.parquet")

    with wr.redshift.connect(secret_id=secret_id, dbname="test") as conn:
        # Get DynamoDB company ids from RedShift
        redshift_df = get_company_from_redshift(schema, conn)

        # Get new companies not in RedShift
        new_company_df = get_new_company(redshift_df, s3_df)

        # Write new companies to RedShift
        if not new_company_df.empty:
            load_company(new_company_df, schema, conn)
            response = {
                "statusCode": 200,
                "body": {
                    "message": "Updated RedShift dim_company",
                    "dynamodbExportId": export_id,
                },
            }
        else:
            response = {
                "statusCode": 200,
                "body": {
                    "message": "No update to RedShift dim_company",
                    "dynamodbExportId": export_id,
                },
            }

    return response
