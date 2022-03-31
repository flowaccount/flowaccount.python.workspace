import os
from datetime import datetime

import awswrangler as wr
import boto3
import pandas as pd

mysql_arn = os.environ["MYSQL_ARN"]
secret_arn = os.environ["MYSQL_SECRET_ARN"]
db_name = os.environ["DB_NAME"]
db_hostname = os.environ["DB_HOSTNAME"]
table = os.environ["TABLE_NAME"]
bucket = os.environ["RAW_BUCKET"]
prefix = os.environ["COUPON_PREFIX"]

s3 = boto3.client("s3")


def handle(event, context):
    print("Get last run time")
    last_run_key = f"{prefix}/last_run.txt"
    try:
        last_run_iso_txt = (
            s3.get_object(Bucket=bucket, Key=last_run_key)["Body"]
            .read()
            .decode("utf-8")
        )
        last_run_dt = datetime.fromisoformat(last_run_iso_txt)
        print("Last run time was", last_run_iso_txt)
    except:
        last_run_dt = None
        print("Last run time not found")

    print("Extract from database")
    conn = wr.data_api.rds.connect(
        resource_arn=mysql_arn,
        database=f"{db_name}@{db_hostname}",
        secret_arn=secret_arn,
    )
    # Build query
    base_query = f"""
        SELECT
            *,
            CURRENT_TIMESTAMP() AS exportTime
        FROM {table}
        """
    if last_run_dt is not None:
        last_run_txt = last_run_dt.strftime("%Y-%m-%d %H:%M:%S")
        where_clause = (
            f"WHERE createon > '{last_run_txt}' OR modifiedOn > '{last_run_txt}'"
        )
    else:
        where_clause = ""

    query = " ".join([base_query, where_clause])
    coupon_df = wr.data_api.rds.read_sql_query(query, con=conn)

    # XXX: Fix "Expected bytes, got a 'bool' object"
    #      Null values are replaced with True boolean during the query
    coupon_df["description"] = coupon_df["description"].apply(
        lambda x: x if not isinstance(x, bool) else pd.NA
    )

    # Convert export time string to datetime
    coupon_df["exportTime"] = pd.to_datetime(coupon_df["exportTime"])

    if coupon_df.shape[0] == 0:
        print("Nothing to write")
        return {
            "status": 200,
            "item_counts": coupon_df.shape[0],
        }

    print("Write data to bucket")
    export_time = coupon_df["exportTime"][0]
    year = export_time.year
    month = export_time.month
    export_type = "full" if last_run_dt is None else "partial"
    file_name = f"{export_type}-{export_time.strftime('%Y-%m-%d-%H-%M-%S')}"
    file_key = f"{prefix}/year={year}/month={month}/{file_name}.parquet"
    wr.s3.to_parquet(df=coupon_df, path=f"s3://{bucket}/{file_key}")

    print("Update last run file")
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/last_run.txt",
        Body=bytes(export_time.isoformat().encode("utf-8")),
    )

    print("Done")
    return {
        "status": 200,
        "export_time": export_time.isoformat(),
        "bucket": bucket,
        "key": file_key,
        "item_counts": coupon_df.shape[0],
    }
