import base64
import json
import os
from typing import List

import awswrangler as wr
import boto3
import hubspot as hs
import pandas as pd
from hubspot.crm.companies import (ApiException,
                                   BatchInputSimplePublicObjectBatchInput)

catalog_db = os.environ["CATALOG_DB"]
catalog_table = os.environ["CATALOG_TABLE"]
secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]
hubspot_schema = os.environ["REDSHIFT_HUBSPOT_SCHEMA"]
hubspot_token_arn = os.environ["HUBSPOT_ACCESS_TOKEN_ARN"]


def get_platform_connection_from_catalog(
    catalog_db: str, catalog_table: str
) -> pd.DataFrame:
    platform_df = wr.s3.read_parquet_table(
        database=catalog_db,
        table=catalog_table,
        columns=["company_id", "platform_name", "is_delete"],
    )
    return platform_df


def get_hubspot_mapping_from_redshift(
    hubspot_schema: str, company_ids: List[int], conn
) -> pd.DataFrame:
    """Get HubSpot mapping from RedShift."""

    query = f"""
            SELECT
                CAST(flowaccount_id AS BIGINT) AS id,
                hubspot_id
            FROM {hubspot_schema}.company_ref
        """

    if company_ids is not None:
        query += f"""
            WHERE id IN {str(company_ids).replace("[", "(").replace("]", ")")}
        """

    hubspot_df = wr.redshift.read_sql_query(query, con=conn)
    return hubspot_df


def aggregate_open_platform_status(
    platform_df: pd.DataFrame, hubspot_df: pd.DataFrame
) -> pd.DataFrame:
    def has_connection(platform: str):
        return lambda x: platform in set(x)

    connected_df = platform_df[~platform_df["is_delete"]]
    merge_df = hubspot_df.merge(
        connected_df, how="left", left_on="id", right_on="company_id"
    )
    merge_df = merge_df[["hubspot_id", "platform_name"]]
    agg_df = merge_df.groupby("hubspot_id").agg(
        has_lazada_connection=pd.NamedAgg(
            column="platform_name", aggfunc=has_connection("lazada")
        ),
        has_shopee_connection=pd.NamedAgg(
            column="platform_name", aggfunc=has_connection("shopee")
        ),
        has_kcash_connection=pd.NamedAgg(
            column="platform_name", aggfunc=has_connection("kcash")
        ),
        has_foodstory_connection=pd.NamedAgg(
            column="platform_name", aggfunc=has_connection("foodstory")
        ),
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
                df["has_foodstory_connection"].map({True: "yes", False: "no"}),
                df["has_kcash_connection"].map({True: "yes", False: "no"}),
                df["has_lazada_connection"].map({True: "yes", False: "no"}),
                df["has_shopee_connection"].map({True: "yes", False: "no"}),
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

    platform_df = get_platform_connection_from_catalog(catalog_db, catalog_table)

    # Get HubSpot mapping
    with wr.redshift.connect(secret_id=secret_arn, dbname=dbname) as conn:
        hubspot_df = get_hubspot_mapping_from_redshift(
            hubspot_schema, list(platform_df["company_id"].drop_duplicates()), conn
        )

    agg_df = aggregate_open_platform_status(platform_df, hubspot_df)

    # Retrieve HubSpot access token
    sm_client = boto3.client("secretsmanager")
    resp = sm_client.get_secret_value(SecretId=hubspot_token_arn)
    if "SecretString" in resp:
        secret = json.loads(resp["SecretString"])
    else:
        secret = json.loads(base64.b64decode(resp["SecretBinary"]))

    # Update HubSpot companies
    hs_client = hs.Client.create(access_token=secret["HUBSPOT_ACCESS_TOKEN"])
    hubspot_batch_update_platform(agg_df, 10, hs_client)

    return {"status": 200}
