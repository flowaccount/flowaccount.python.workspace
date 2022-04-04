import base64
import json
import logging
import os

import boto3
import psycopg2

COUPON_NA_KEY = 1

secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]
dim_schema = os.environ["DIM_SCHEMA"]
dim_table = os.environ["DIM_TABLE"]
fact_schema = os.environ["FACT_SCHEMA"]
fact_table = os.environ["FACT_TABLE"]

sm = boto3.client("secretsmanager")


def handle(event, context):
    logging.info("Retrieving RedShift credential")
    resp = sm.get_secret_value(SecretId=secret_arn)
    if "SecretString" in resp:
        secret = json.loads(resp["SecretString"])
    else:
        secret = json.loads(base64.b64decode(resp["SecretBinary"]))
    host = secret["host"]
    port = secret["port"]
    user = secret["username"]
    password = secret["password"]

    logging.info("Connecting to RedShift")
    with psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    ) as conn:
        with conn.cursor() as cursor:
            logging.info("Fill missing coupons")
            cursor.execute(
                f"""
                UPDATE {fact_schema}.{fact_table} AS f
                SET
                coupon_key = {COUPON_NA_KEY}
                WHERE f.coupon IS NULL AND f.coupon_key IS NULL;
            """
            )

            logging.info("Fill coupon keys")
            cursor.execute(
                f"""
                UPDATE {fact_schema}.{fact_table} AS f
                SET
                    coupon_key = d.coupon_key
                FROM {dim_schema}.{dim_table} AS d
                WHERE f.coupon_key IS NULL AND d.mysql_id = f.coupon;
            """
            )

        logging.info("Commit")
        conn.commit()

    logging.info("Done")
    return {"status": 200}
