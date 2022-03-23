import base64
import json
import os
import urllib.parse
from typing import List, TypedDict

import boto3
import hubspot
from hubspot.crm.companies import (ApiException,
                                   BatchInputSimplePublicObjectBatchInput)

access_token_arn = os.environ["HUBSPOT_ACCESS_TOKEN_ARN"]
step_size = int(os.environ["HUBSPOT_BATCH_UPDATE_SIZE"])

s3 = boto3.client("s3")


class HubspotUpdateInput(TypedDict):
    id: int
    properties: dict


def get_hubspot_token(secret_arn: str):
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_arn)
    if "SecretString" in resp:
        secret = json.loads(resp["SecretString"])
    else:
        secret = json.loads(base64.b64decode(resp["SecretBinary"]))

    return secret["HUBSPOT_ACCESS_TOKEN"]


def batch_company_update(inputs: List[HubspotUpdateInput]):
    access_token = get_hubspot_token(access_token_arn)
    hs = hubspot.Client.create(access_token=access_token)
    row_count = len(inputs)

    for cur_start in range(0, row_count, step_size):
        if cur_start + step_size <= row_count:
            cur_end = cur_start + step_size
        else:
            cur_end = row_count

        hs_input = BatchInputSimplePublicObjectBatchInput(
            inputs=inputs[cur_start:cur_end]
        )
        try:
            hs.client.crm.companies.batch_api.update(
                batch_input_simple_public_object_batch_input=hs_input
            )
        except ApiException as e:
            print("Exception when calling batch_api->update: %s\n" % e)


def handle(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(f"s3 create event: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)["Body"]
    lines = obj.read().decode("utf-8")
    inputs = [json.loads(line) for line in lines]

    batch_company_update(inputs)
    return {"status": 200}
