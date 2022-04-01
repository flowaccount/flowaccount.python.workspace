import os

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import ClientError

clean_db = os.environ["CLEAN_CATALOG"]
clean_table = os.environ["CLEAN_TABLE"]
clean_bucket = os.environ["CLEAN_BUCKET"]
clean_prefix = os.environ["CLEAN_TABLE_PREFIX"]

glue = boto3.client("glue")


def handle(event, context):
    delta_bucket = event["bucket"]
    delta_file_key = event["key"]

    print(f"Get delta file s3://{delta_bucket}/{delta_file_key}")
    delta_df = wr.s3.read_parquet(f"s3://{delta_bucket}/{delta_file_key}")

    if delta_df.shape[0] == 0:
        print("Delta file is empty")
        return {"status": 200}

    # Check if catalog existed
    is_table_existed = True
    try:
        glue.get_table(DatabaseName=clean_db, Name=clean_table)
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            is_table_existed = False
        else:
            raise e

    if is_table_existed:
        # Upsert and keep the most recent row
        cur_df = wr.s3.read_parquet_table(database=clean_db, table=clean_table)
        new_df = (
            pd.concat([cur_df, delta_df])
            .sort_values(["modified_on", "created_on"], asecending=False)
            .drop_duplicates(subset=["id"])
        )
    else:
        new_df = delta_df

    if is_table_existed:
        print(f"Upsert table {clean_db}.{clean_table}")
    else:
        print(f"Create table {clean_db}.{clean_table}")
    result = wr.s3.to_parquet(
        df=new_df,
        path=f"s3://{clean_bucket}/{clean_prefix}",
        dataset=True,
        mode="overwrite",
        database=clean_db,
        table=clean_table,
    )

    return {"status": 200, "item_counts": new_df.shape[0], "paths": result["paths"]}
