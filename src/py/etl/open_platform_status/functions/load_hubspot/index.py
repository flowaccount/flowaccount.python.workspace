import base64
import os

import awswrangler as wr
import boto3
import hubspot as hs
import pandas as pd
from hubspot.crm.companies import (ApiException,
                                   BatchInputSimplePublicObjectBatchInput)
from redshift_connector import Connection as RedShiftConnection

secret_id = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]
platform_schema = os.environ["REDSHIFT_PLATFORM_SCHEMA"]
hubspot_schema = os.environ["REDSHIFT_HUBSPOT_SCHEMA"]
hubspot_token_arn = os.environ["HUBSPOT_ACCESS_TOKEN_ARN"]


def get_platform_from_redshift(
    platform_schema: str, hubspot_schema: str, conn: RedShiftConnection
) -> pd.DataFrame:
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
                FROM {platform_schema}.fact_open_platform_connection
            )
            SELECT
                h.flowaccount_id AS flowaccount_id,
                h.hubspot_id AS hubspot_id,
                f.platform AS platform,
                f.status AS status
            FROM cte_1 AS f
            JOIN {platform_schema}.dim_company AS c ON c.company_key = f.company_key
            JOIN {hubspot_schema}.company_ref AS h ON h.flowaccount_id = c.dynamodb_key
            WHERE row_num = 1
        """,
        con=conn,
    )

    redshift_df["platform"] = redshift_df["platform"].astype("category")

    return redshift_df


def agg_platform(name: str):
    def _agg_platform(s: pd.Series):
        if not s[s == name].empty:
            return "yes"
        else:
            return "no"

    return _agg_platform


def aggregate_platform_by_company(redshift_df: pd.DataFrame):
    agg_df = redshift_df.groupby("hubspot_id").agg(
        foodstory_api=pd.NamedAgg(
            column="platform", aggfunc=agg_platform("Food Story")
        ),
        k_cash_connect_api=pd.NamedAgg(
            column="platform", aggfunc=agg_platform("K-Cash")
        ),
        lazada_api=pd.NamedAgg(column="platform", aggfunc=agg_platform("Lazada")),
        shopee_api=pd.NamedAgg(column="platform", aggfunc=agg_platform("Shopee")),
    )
    return agg_df.reset_index()


def convert_open_platform_status_to_hubspot_inputs(df: pd.DataFrame):
    return BatchInputSimplePublicObjectBatchInput(
        [
            {
                "id": hubspot_id,
                "properties": {
                    "foodstory_api": foodstory_api,
                    "k_cash_connect_api": k_cash_connect_api,
                    "lazada_api": lazada_api,
                    "shopee_api": shopee_api,
                },
            }
            for hubspot_id, foodstory_api, k_cash_connect_api, lazada_api, shopee_api in zip(
                df["hubspot_id"],
                df["foodstory_api"],
                df["k_cash_connect_api"],
                df["lazada_api"],
                df["shopee_api"],
            )
        ]
    )


def hubspot_batch_update_platform(
    agg_df: pd.DataFrame, step_size: int, client: hs.Client
):
    row_count = agg_df.shape[0]
    for cur_start in range(0, row_count, step_size):
        if cur_start + step_size <= row_count:
            cur_end = cur_start + step_size
        else:
            cur_end = row_count

        inputs = convert_open_platform_status_to_hubspot_inputs(
            agg_df.iloc[cur_start:cur_end]
        )
        try:
            api_response = client.crm.companies.batch_api.update(
                batch_input_simple_public_object_batch_input=inputs
            )
            print(api_response)
        except ApiException as e:
            print("Exception when calling batch_api->update: %s\n" % e)


def handle(event, context):
    """Load latest open platform status to HubSpot."""

    # Get platform status for all HubSpot companies
    with wr.redshift.connect(secret_id=secret_id, dbname=dbname) as conn:
        redshift_df = get_platform_from_redshift(platform_schema, hubspot_schema, conn)
    agg_df = aggregate_platform_by_company(redshift_df)

    # Retrieve HubSpot access token
    sm_client = boto3.client("secretsmanager")
    resp = sm_client.get_secret_value(SecretId=hubspot_token_arn)
    if "SecretString" in resp:
        access_token = resp["SecretString"]
    else:
        access_token = base64.b64decode(resp["SecretBinary"])

    # Update HubSpot companies
    hs_client = hs.Client.create(access_token=access_token)
    hubspot_batch_update_platform(agg_df, 10, hs_client)

    return {"status": 200}
