from typing import List

import pandas as pd
from flowaccount.utils import format_snake_case


def clean_open_platform_cdc(cdc_list: List[dict]) -> pd.DataFrame:
    """Clean DynamoDB open-platform-company-user-v2 table's CDC."""

    raw_df = pd.DataFrame(cdc_list)

    # Extract fields in dynamodb column
    dynamodb_df = pd.json_normalize(clean_df["dynamodb"], max_level=0)
    clean_df = pd.concat([raw_df, dynamodb_df], axis=1)
    clean_df = clean_df.drop(columns=["dynamodb"])

    # Choose current record data based on event type
    clean_df["Image"] = clean_df.apply(
        lambda x: x["NewImage"]
        if x["eventName"] == "INSERT" or x["eventName"] == "MODIFY"
        else x["OldImage"],
        axis=1,
    )
    clean_df = clean_df.drop(columns=["NewImage", "OldImage"])

    # Drop Keys column too because their values are already included in Image column
    clean_df = clean_df.drop(columns=["Keys"])

    # Extract fields in Image column
    image_df = pd.json_normalize(clean_df["Image"]).rename(
        columns=lambda x: x.rsplit(".")[0]
    )
    clean_df = pd.concat([clean_df, image_df], axis=1)
    clean_df = clean_df.drop(columns=["Image"])

    # Convert pascal case to camel case
    clean_df = clean_df.rename(
        columns=lambda x: x[0].lower() + x[1:] if x[0].isupper() else x
    )
    clean_df = clean_df.rename(columns={"eventID": "event_id"})

    # Convert camel case to snake case
    clean_df = clean_df.rename(columns=format_snake_case)

    # Select columns for output
    record_cols = list(image_df.columns.values)
    clean_df = clean_df[
        ["event_id", "event_name", "table_name", "approximate_creation_date_time"]
        + record_cols
    ]

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
