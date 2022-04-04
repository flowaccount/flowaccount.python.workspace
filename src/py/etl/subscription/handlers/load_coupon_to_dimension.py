import base64
import json
import logging
import os

import boto3
import psycopg2

secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
catalog_table = os.environ["CLEAN_TABLE"]
dbname = os.environ["REDSHIFT_DB"]
spectrum_schema = os.environ["REDSHIFT_SPECTRUM_SCHEMA"]
dim_schema = os.environ["REDSHIFT_DIM_SCHEMA"]
dim_table = os.environ["REDSHIFT_DIM_TABLE"]
staging_table = "coupon_staging"

sm = boto3.client("secretsmanager")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

    logger.info("Connecting to RedShift")
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

    logger.info("Begin transaction")
    with conn.cursor() as cursor:
        logger.info("Creating staging table")
        cursor.execute(
            f"""
            CREATE TEMP TABLE {staging_table}
            (LIKE {dim_schema}.{dim_table} INCLUDING DEFAULTS)
        """
        )
        cursor.execute(
            f"""
            INSERT INTO {staging_table}(
                mysql_id, code, description,
                renew_applicable, change_applicable, new_applicable,
                discount_type, discount_value,
                coupon_valid_start, coupon_valid_end,
                active, coupon_deleted,
                coupon_created_on, created_by,
                coupon_modified_on, modified_by
            )
            (SELECT
                id, code, description,
                renew_type, change_type, new_type,
                discount_type, discount_value,
                start_date, end_date,
                active, is_delete,
                created_on, created_by,
                modified_on, modified_by
            FROM {spectrum_schema}.{catalog_table})
        """
        )

        logger.info("Updating rows")
        cursor.execute(
            f"""
            UPDATE {dim_schema}.{dim_table} AS dst
            SET
                mysql_id = staging.mysql_id,
                code = staging.code,
                description = staging.description,
                renew_applicable = staging.renew_applicable,
                change_applicable = staging.change_applicable,
                new_applicable = staging.new_applicable,
                discount_type = staging.discount_type,
                discount_value = staging.discount_value,
                coupon_valid_start = staging.coupon_valid_start,
                coupon_valid_end = staging.coupon_valid_end,
                active = staging.active,
                coupon_deleted = staging.coupon_deleted,
                coupon_created_on = staging.coupon_created_on,
                created_by = staging.created_by,
                coupon_modified_on = staging.coupon_modified_on,
                modified_by = staging.modified_by
            FROM {staging_table} AS staging
            WHERE staging.mysql_id = dst.mysql_id
            """
        )
        cursor.execute(
            f"""
            DELETE FROM {staging_table}
            USING {dim_schema}.{dim_table}
            WHERE {staging_table}.mysql_id = {dim_schema}.{dim_table}.mysql_id
        """
        )

        logger.info("Inserting rows")
        cursor.execute(
            f"""
            INSERT INTO {dim_schema}.{dim_table}(
                mysql_id, code, description,
                renew_applicable, change_applicable, new_applicable,
                discount_type, discount_value,
                coupon_valid_start, coupon_valid_end,
                active, coupon_deleted,
                coupon_created_on, created_by,
                coupon_modified_on, modified_by
            )
            (SELECT
                mysql_id, code, description,
                renew_applicable, change_applicable, new_applicable,
                discount_type, discount_value,
                coupon_valid_start, coupon_valid_end,
                active, coupon_deleted,
                coupon_created_on, created_by,
                coupon_modified_on, modified_by
            FROM {staging_table})
        """
        )

        logger.info("Dropping staging table")
        cursor.execute(
            f"""
            DROP TABLE {staging_table}
        """
        )

    logger.info("End transaction")
    conn.commit()

    logger.info("Closing connection")
    conn.close()

    return {"status": 200}
