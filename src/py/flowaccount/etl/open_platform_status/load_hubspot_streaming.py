import json
from typing import List, Tuple, Union

import awswrangler as wr
import pandas as pd
from redshift_connector import Connection as RedShiftConnection

platform_mapping = {
    "Lazada": "lazada_api",
    "Shopee": "shopee_api",
    "K-Cash": "k_cash_connect_api",
    "FoodStory": "foodstory_api",
}
supported_platforms = list(platform_mapping.keys())


def get_hubspot_mapping(
    companies: list, schema: str, table: str, conn: RedShiftConnection
) -> pd.DataFrame:
    """Get HubSpot FlowAccount HubSpot company ID mapping from RedShift."""

    rs_df = wr.redshift.read_sql_query(
        f"""
        SELECT hubspot_id, flowaccount_id AS company_id
        FROM {schema}.{table}
        WHERE company_id IN {str(companies)}
        """,
        con=conn,
    )

    return rs_df


def attach_hubspot_id(
    cdc_df: pd.DataFrame, mapping_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Attach HubSpot ID to CDC dataframe."""

    df = pd.merge(cdc_df, mapping_df, how="left", on="company_id")
    connection_df = df[df["hubspot_id"].notna()]
    missing_rel_df = df[df["hubspot_id"].isna()]
    return connection_df, missing_rel_df


def filter_platform(
    connection_df: pd.DataFrame, platforms: list = supported_platforms
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    platform_df = connection_df[connection_df["platform_name"].isin(platforms)]
    missing_platform_df = connection_df[~connection_df["platform_name"].isin(platforms)]
    return platform_df, missing_platform_df


def filter_event(platform_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    event_df = platform_df[platform_df["event_name"].isin(["INSERT", "REMOVE"])]
    invalid_event_df = platform_df[
        ~platform_df["event_name"].isin(["INSERT", "REMOVE"])
    ]
    return event_df, invalid_event_df


def aggregate_latest_status(event_df: pd.DataFrame) -> pd.DataFrame:
    """Get latest platfrom status for each HubSpot ID."""

    df = (
        event_df.sort_values("approximate_creation_date_time")
        .groupby(["hubspot_id", "platform_name"])
        .last()
        .reset_index("platform_name")
    )
    df["status"] = df["event_name"].map({"INSERT": "yes", "REMOVE": "no"})
    df["hubspot_key"] = df["platform_name"].map(platform_mapping)
    df = df[["hubspot_key", "status"]]

    return df


def convert_to_platform_status_dict(status_df: Union[pd.DataFrame, pd.Series]) -> dict:
    if isinstance(status_df, pd.DataFrame):
        return dict(zip(status_df["hubspot_key"], status_df["status"]))
    else:
        return dict([(status_df["hubspot_key"], status_df["status"])])


def convert_to_json_line(inputs: List[dict]) -> str:
    """Convert a list of dict into one line one json string."""

    return "\n".join([json.dumps(input) for input in inputs])
