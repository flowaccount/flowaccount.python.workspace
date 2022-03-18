from typing import List

import pandas as pd
from flowaccount.utils import format_snake_case

CLEAN_CDC_DTYPES = {
    # Partition columns
    "year": "Int64",
    "month": "Int64",
    # CDC metadata
    "event_id": "string",
    "event_name": "category",
    "table_name": "string",
    "approximate_creation_date_time": "datetime64",
    # Known record columns
    "company_id": "Int64",
    "shop_id": "string",
    "is_delete": "boolean",
    "user_id": "Int64",
    "platform_name": "string",
    "platform_info": "string",
    "expired_at": "datetime64",
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

CLEAN_CDC_COLUMNS = list(CLEAN_CDC_DTYPES.keys())


def clean_open_platform_cdc(cdc_list: List[dict]) -> pd.DataFrame:
    """Clean DynamoDB open-platform-company-user-v2 table's CDC."""

    # Create output dataframe with known columns
    clean_df = pd.DataFrame(columns=CLEAN_CDC_COLUMNS)

    # If list is empty, then return empty dataframe
    if len(cdc_list) == 0:
        return clean_df

    raw_df = pd.DataFrame(
        cdc_list,
        columns=[
            "awsRegion",
            "eventID",
            "eventName",
            "userIdentity",
            "recordFormat",
            "tableName",
            "dynamodb",
            "eventSource",
        ],
    )

    # Extract fields in dynamodb column
    dynamodb_df = pd.concat(
        [
            pd.DataFrame(
                columns=[
                    "ApproximateCreationDateTime",
                    "Keys",
                    "NewImage",
                    "OldImage",
                    "SizeBytes",
                ]
            ),
            pd.json_normalize(raw_df["dynamodb"], max_level=0),
        ]
    )
    df = pd.concat([raw_df, dynamodb_df], axis=1)
    df = df.drop(columns=["dynamodb"])

    # Consolidate record data
    df["NewImage"] = df["NewImage"].fillna(df["OldImage"])
    df = df.rename(columns={"NewImage": "Image"})
    df = df.drop(columns=["OldImage"])

    # Drop Keys column too because their values are already included in Image column
    df = df.drop(columns=["Keys"])

    # Extract fields in Image column
    image_df = pd.json_normalize(df["Image"]).rename(columns=lambda x: x.rsplit(".")[0])
    df = pd.concat([df, image_df], axis=1)
    df = df.drop(columns=["Image"])

    # Convert pascal case to camel case
    df = df.rename(columns=lambda x: x[0].lower() + x[1:] if x[0].isupper() else x)
    df = df.rename(columns={"eventID": "event_id"})

    # Convert camel case to snake case
    df = df.rename(columns=format_snake_case)

    # Insert records with arbitrary columns
    clean_df = pd.concat([clean_df, df])

    # Drop unneeded columns
    clean_df = clean_df.drop(
        columns=[
            "aws_region",
            "user_identity",
            "record_format",
            "event_source",
            "size_bytes",
            "year",
            "month",
        ]
    )

    # Convert int columns
    int_cols = [
        "company_id",
        "user_id",
        "payment_channel_id",
        "expires_in",
        "refresh_expires_in",
    ]
    for col in int_cols:
        clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

    # Convert datetime columns
    clean_df["approximate_creation_date_time"] = pd.to_datetime(
        clean_df["approximate_creation_date_time"], unit="ms"
    )
    clean_df["expired_at"] = pd.to_datetime(clean_df["expired_at"], unit="s")
    clean_df["created_at"] = pd.to_datetime(clean_df["created_at"], unit="s")
    clean_df["updated_at"] = pd.to_datetime(clean_df["updated_at"], unit="s")

    # Add columns for partitioning
    clean_df["year"] = clean_df["approximate_creation_date_time"].dt.year
    clean_df["month"] = clean_df["approximate_creation_date_time"].dt.month

    # Map platform name
    clean_df["platform_name"] = clean_df["platform_name"].map(
        {"lazada": "Lazada", "shopee": "Shopee"}
    )

    # Enforce data types
    clean_df = clean_df.astype(CLEAN_CDC_DTYPES)

    return clean_df
