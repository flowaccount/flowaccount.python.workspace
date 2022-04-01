import base64
import json
import os

import boto3
import psycopg2

secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
dbname = os.environ["REDSHIFT_DB"]
sm = boto3.client("secretsmanager")


def handle(event, context):
    resp = sm.get_secret_value(SecretId=secret_arn)
    if "SecretString" in resp:
        secret = json.loads(resp["SecretString"])
    else:
        secret = json.loads(base64.b64decode(resp["SecretBinary"]))
    host = secret["host"]
    port = secret["port"]
    user = secret["username"]
    password = secret["password"]
    dbname = "etl"

    with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
        with conn.cursor() as cursor:
            print("Fill missing coupons")
            cursor.execute(
                """
                UPDATE etl.subscriptions AS f
                SET
                coupon_key = 1
                WHERE f.coupon IS NULL AND f.coupon_key IS NULL;
            """
            )

            print("Fill coupon keys")
            cursor.execute(
                """
                UPDATE etl.subscriptions AS f
                SET
                    coupon_key = d.coupon_key
                FROM dim.dim_coupon AS d
                WHERE f.coupon_key IS NULL AND d.mysql_id = f.coupon;
            """
            )

        print("Commit")
        conn.commit()

    print("Done")
    return {"status": 200}
