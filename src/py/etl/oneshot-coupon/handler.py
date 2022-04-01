import os

import awswrangler as wr
import pandas as pd

secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]


def handle(event, context):
    coupon_df = pd.read_parquet("coupon.parquet")
    coupon_df = coupon_df.rename(
        columns={
            "id": "mysql_id",
            "code": "code",
            "description": "description",
            "renew_type": "renew_applicable",
            "change_type": "change_applicable",
            "new_type": "new_applicable",
            "discount_type": "discount_type",
            "discount_value": "discount_value",
            "start_date": "coupon_valid_start",
            "end_date": "coupon_valid_end",
            "is_delete": "coupon_deleted",
            "created_on": "coupon_created_on",
            "created_by": "created_by",
            "modified_on": "coupon_modified_on",
            "modified_by": "modified_by",
            "active": "active",
        }
    )

    with wr.redshift.connect(secret_id=secret_arn, dbname=dbname) as conn:
        wr.redshift.to_sql(
            coupon_df,
            schema="dim",
            table="dim_coupon",
            mode="append",
            use_column_names=True,
            con=conn,
        )

    return {"status": 200}
