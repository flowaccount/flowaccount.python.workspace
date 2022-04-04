import logging
import os

import awswrangler as wr

secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
catalog = os.environ["CLEAN_CATALOG"]
catalog_table = os.environ["CLEAN_TABLE"]
dbname = os.environ["REDSHIFT_DB"]
dim_schema = os.environ["REDSHIFT_DIM_SCHEMA"]
dim_table = os.environ["REDSHIFT_DIM_TABLE"]


def handle(event, context):
    logging.info(f"Reading data from catalog {catalog}.{catalog_table}")
    coupon_df = wr.s3.read_parquet_table(database=catalog, table=catalog_table)
    coupon_df.rename(
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

    logging.info(f"Loading RedShift coupon dimension {dbname}.{dim_schema}.{dim_table}")
    with wr.redshift.connect(secret_id=secret_arn, dbname=dbname) as conn:
        wr.redshift.to_sql(
            df=coupon_df,
            schema=dim_schema,
            table=dim_table,
            mode="upsert",
            primary_keys=["mysql_id"],
            use_column_names=True,
            con=conn,
        )

    return {"status": 200}
