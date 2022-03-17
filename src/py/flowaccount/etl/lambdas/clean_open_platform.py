from typing import List

import pandas as pd
from flowaccount.utils import format_snake_case


def clean_open_platform_cdc(cdc_list: List[dict]) -> pd.DataFrame:
    """Clean DynamoDB open-platform-company-user-v2 table's CDC."""

    raw_df = pd.DataFrame(cdc_list)

    # Extract fields in dynamodb column
    dynamodb_df = pd.json_normalize(df["dynamodb"], max_level=0)
    df = pd.concat([raw_df, dynamodb_df], axis=1)
    df = df.drop(columns=["dynamodb"])

    # Choose current record data based on event type
    df["Image"] = df.apply(
        lambda x: x["NewImage"]
        if x["eventName"] == "INSERT" or x["eventName"] == "MODIFY"
        else x["OldImage"],
        axis=1,
    )
    df = df.drop(columns=["NewImage", "OldImage"])

    # Drop Keys column too because their values are already included in Image column
    df = df.drop(columns=["Keys"])

    # Extract fields in Image column
    image_df = pd.json_normalize(df["Image"]).rename(
        columns=lambda x: x.rsplit(".")[0]
    )
    df = pd.concat([df, image_df], axis=1)
    df = df.drop(columns=["Image"])

    # Convert pascal case to camel case
    df = df.rename(
        columns=lambda x: x[0].lower() + x[1:] if x[0].isupper() else x
    )
    df = df.rename(columns={"eventID": "event_id"})

    # Convert camel case to snake case
    df = df.rename(columns=format_snake_case)

    # Create output dataframe with known columns
    clean_df = pd.DataFrame(columns=[
        # CDC metadata
        "event_id", "event_name", "table_name", "approximate_creation_date_time",
        # Known record columns
        "company_id",
        "shop_id",
        "is_delete",
        "user_id",
        "platform_name",
        "platform_info",
        "expired_at",
        "payment_channel_id",
        "created_at",
        "expires_in",
        "is_vat",
        "payload",
        "guid",
        "refresh_expires_in",
        "updated_at",
        "refresh_token",
        "remarks",
        "access_token",
        ])

    # Insert records with arbitrary columns
    clean_df = pd.concat([clean_df, df])

    # Enforce data types
    clean_df["approximate_creation_date_time"] = pd.to_datetime(
        clean_df["approximate_creation_date_time"], unit="ms"
    )
    clean_df["expired_at"] = pd.to_datetime(clean_df["expired_at"], unit="s")
    clean_df["created_at"] = pd.to_datetime(clean_df["created_at"], unit="s")
    clean_df["updated_at"] = pd.to_datetime(clean_df["updated_at"], unit="s")
    clean_df = clean_df.astype(
        {
            # CDC metadata
            "event_id": "string",
            "event_name": "category",
            "table_name": "string",
            "approximate_creation_date_time": "datetime64",
            # Known record columns
            "company_id": "int64",
            "shop_id": "string",
            "is_delete": "boolean",
            "user_id": "int64",
            "platform_name": "string",
            "platform_info": "string",
            "expired_at": "Int64",
            "payment_channel_id": "Int64",
            "created_at": "datetime64",
            "expires_in": "Int64",
            "is_vat": "boolean",
            "payload": "string",
            "guid": "string",
            "refresh_expires_in": "Int64",
            "updated_at": "datetime64",
            "refresh_token": "string",
            "remarks": "string",
            "access_token": "string",
        }
    )

    # Add columns for partitioning
    clean_df["year"] = clean_df["approximate_creation_date_time"].dt.year
    clean_df["month"] = clean_df["approximate_creation_date_time"].dt.month

    return clean_df
