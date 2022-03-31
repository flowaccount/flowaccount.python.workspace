import os
from pathlib import Path

import awswrangler as wr
import pandas as pd

bucket = os.environ["CLEAN_BUCKET"]
prefix = os.environ["COUPON_PREFIX"]


def clean_coupon(raw_df: pd.DataFrame) -> pd.DataFrame:
    # Copy dataframe
    df = pd.DataFrame(raw_df)

    # Map values
    df["discountType"] = df["discountType"].map({1: "Percent", 3: "Amount"})
    df["active"] = df["status"].map({1: True})

    # Fill missing values
    df = df.fillna(
        {
            "description": "",
            "renewType": False,
            "changeType": False,
            "newType": False,
            "discountType": "Undefined",
            "active": False,
            "isDelete": False,
        }
    )

    # Rename columns
    df = df.rename(
        columns={
            "id": "id",
            "code": "code",
            "description": "description",
            "renewType": "renew_type",
            "changeType": "change_type",
            "newType": "new_type",
            "discountType": "discount_type",
            "discountValue": "discount_value",
            "startDate": "start_date",
            "endDate": "end_date",
            "status": "status",
            "isDelete": "is_delete",
            "createon": "created_on",
            "createdBy": "created_by",
            "modifiedOn": "modified_on",
            "modifiedBy": "modified_by",
            "exportTime": "export_time",
            "active": "active",
        }
    )

    # Select columns
    df = df[
        [
            "id",
            "code",
            "description",
            "renew_type",
            "change_type",
            "new_type",
            "discount_type",
            "discount_value",
            "start_date",
            "end_date",
            "is_delete",
            "created_on",
            "created_by",
            "modified_on",
            "modified_by",
            "export_time",
            "active",
        ]
    ]

    df = df.astype(
        {
            "id": "Int64",
            "code": "string",
            "description": "string",
            "renew_type": "boolean",
            "change_type": "boolean",
            "new_type": "boolean",
            "discount_type": "category",
            "discount_value": "object",
            "start_date": "datetime64[ns]",
            "end_date": "datetime64[ns]",
            "is_delete": "boolean",
            "created_on": "datetime64[ns]",
            "created_by": "string",
            "modified_on": "datetime64[ns]",
            "modified_by": "string",
            "export_time": "datetime64[ns]",
            "active": "boolean",
        }
    )

    return df


def handle(event, context):
    print("Get raw coupon file")
    raw_bucket = event["bucket"]
    raw_file_key = event["key"]
    raw_df = wr.s3.read_parquet(f"s3://{raw_bucket}/{raw_file_key}")

    if raw_df is None:
        print(f"File not found - s3://{raw_bucket}/{raw_file_key}")
        return {"status": 400, "raw_bucket": raw_bucket, "raw_file_key": raw_file_key}

    print("Clean coupon file")
    clean_df = clean_coupon(raw_df)

    print("Upload cleaned file")
    export_time = clean_df["export_time"][0]
    year = export_time.year
    month = export_time.month
    file_name = Path(raw_file_key).name.split(".")[0]
    file_key = f"{prefix}/year={year}/month={month}/{file_name}.parquet"
    wr.s3.to_parquet(
        df=clean_df,
        path=f"s3://{bucket}/{file_key}",
    )

    print("Done")
    return {
        "status": 200,
        "export_time": export_time.isoformat(),
        "bucket": bucket,
        "key": file_key,
        "item_counts": clean_df.shape[0],
    }
