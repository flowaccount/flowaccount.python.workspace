from datetime import datetime, timezone
from unittest import TestCase

import pandas as pd
import pandas.testing as pdtest
from flowaccount.etl.lambdas.clean_open_platform import clean_open_platform_cdc


class CleanOpenPlatformCdcTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.clean_cdc_types = {
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

        cls.clean_cdc_columns = list(cls.clean_cdc_types.keys())

        return super().setUpClass()

    def test_has_mandatory_columns(self):
        expected = pd.DataFrame(columns=self.clean_cdc_columns)
        result = clean_open_platform_cdc([])
        pdtest.assert_frame_equal(result, expected)

    def test_clean_insert_change_succeeds(self):
        data = {
            "awsRegion": "ap-southeast-1",
            "eventID": "76e8e009-7e43-4716-8be1-1441391439aa",
            "eventName": "INSERT",
            "userIdentity": None,
            "recordFormat": "application/json",
            "tableName": "flowaccount-open-platform-company-user-v2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1646021721575,
                "Keys": {"companyId": {"N": "9999"}, "shopId": {"S": "0"}},
                "NewImage": {
                    "isDelete": {"BOOL": False},
                    "platformName": {"S": "lazada"},
                    "shopId": {"S": "0"},
                    "companyId": {"N": "9999"},
                    "userId": {"N": "9999"},
                },
                "SizeBytes": 74,
            },
            "eventSource": "aws:dynamodb",
        }
        cleaned_data = {
            # Partition columns
            "year": 2022,
            "month": 2,
            # CDC metadata
            "event_id": "76e8e009-7e43-4716-8be1-1441391439aa",
            "event_name": "INSERT",
            "table_name": "flowaccount-open-platform-company-user-v2",
            "approximate_creation_date_time": datetime.utcfromtimestamp(
                1646021721575 / 1000.0
            ),
            # Known record columns
            "company_id": 9999,
            "shop_id": "0",
            "is_delete": False,
            "user_id": 9999,
            "platform_name": "Lazada",
            "platform_info": None,
            "expired_at": None,
            "payment_channel_id": None,
            "created_at": None,
            "expires_in": None,
            "is_vat": None,
            "payload": None,
            "guid": None,
            "refresh_expires_in": None,
            "updated_at": None,
            "refresh_token": None,
            "remarks": None,
            "access_token": None,
        }
        expected = pd.DataFrame([cleaned_data]).astype(self.clean_cdc_types)
        result = clean_open_platform_cdc([data])
        pdtest.assert_frame_equal(
            result.sort_index(axis=1), expected.sort_index(axis=1)
        )

    def test_clean_modify_change_succeeds(self):
        data = {
            "awsRegion": "ap-southeast-1",
            "eventID": "4e4974e9-53fb-4559-82a2-c060b16e57a5",
            "eventName": "MODIFY",
            "userIdentity": None,
            "recordFormat": "application/json",
            "tableName": "flowaccount-open-platform-company-user-v2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1646025622195,
                "Keys": {"companyId": {"N": "9999"}, "shopId": {"S": "0"}},
                "NewImage": {
                    "isDelete": {"BOOL": False},
                    "platformName": {"S": "shopee"},
                    "shopId": {"S": "0"},
                    "companyId": {"N": "9999"},
                    "userId": {"N": "9999"},
                },
                "OldImage": {
                    "isDelete": {"BOOL": False},
                    "platformName": {"S": "lazada"},
                    "shopId": {"S": "0"},
                    "companyId": {"N": "9999"},
                    "userId": {"N": "9999"},
                },
                "SizeBytes": 129,
            },
            "eventSource": "aws:dynamodb",
        }
        cleaned_data = {
            # Partition columns
            "year": 2022,
            "month": 2,
            # CDC metadata
            "event_id": "4e4974e9-53fb-4559-82a2-c060b16e57a5",
            "event_name": "MODIFY",
            "table_name": "flowaccount-open-platform-company-user-v2",
            "approximate_creation_date_time": datetime.utcfromtimestamp(
                1646025622195 / 1000.0
            ),
            # Known record columns
            "company_id": 9999,
            "shop_id": "0",
            "is_delete": False,
            "user_id": 9999,
            "platform_name": "Shopee",
            "platform_info": None,
            "expired_at": None,
            "payment_channel_id": None,
            "created_at": None,
            "expires_in": None,
            "is_vat": None,
            "payload": None,
            "guid": None,
            "refresh_expires_in": None,
            "updated_at": None,
            "refresh_token": None,
            "remarks": None,
            "access_token": None,
        }
        expected = pd.DataFrame([cleaned_data]).astype(self.clean_cdc_types)
        result = clean_open_platform_cdc([data])
        pdtest.assert_frame_equal(
            result.sort_index(axis=1), expected.sort_index(axis=1)
        )

    def test_clean_remove_change_succeeds(self):
        data = {
            "awsRegion": "ap-southeast-1",
            "eventID": "78809f57-959a-4c6b-88ed-fa42c26b984d",
            "eventName": "REMOVE",
            "userIdentity": None,
            "recordFormat": "application/json",
            "tableName": "flowaccount-open-platform-company-user-v2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1646024785597,
                "Keys": {"companyId": {"N": "9999"}, "shopId": {"S": "0"}},
                "OldImage": {
                    "isDelete": {"BOOL": False},
                    "shopId": {"S": "0"},
                    "companyId": {"N": "9999"},
                    "userId": {"N": "9999"},
                    "platformName": {"S": "lazada"},
                },
                "SizeBytes": 74,
            },
            "eventSource": "aws:dynamodb",
        }
        cleaned_data = {
            # Partition columns
            "year": 2022,
            "month": 2,
            # CDC metadata
            "event_id": "78809f57-959a-4c6b-88ed-fa42c26b984d",
            "event_name": "REMOVE",
            "table_name": "flowaccount-open-platform-company-user-v2",
            "approximate_creation_date_time": datetime.utcfromtimestamp(
                1646024785597 / 1000.0
            ),
            # Known record columns
            "company_id": 9999,
            "shop_id": "0",
            "is_delete": False,
            "user_id": 9999,
            "platform_name": "Lazada",
            "platform_info": None,
            "expired_at": None,
            "payment_channel_id": None,
            "created_at": None,
            "expires_in": None,
            "is_vat": None,
            "payload": None,
            "guid": None,
            "refresh_expires_in": None,
            "updated_at": None,
            "refresh_token": None,
            "remarks": None,
            "access_token": None,
        }
        expected = pd.DataFrame([cleaned_data]).astype(self.clean_cdc_types)
        result = clean_open_platform_cdc([data])
        pdtest.assert_frame_equal(
            result.sort_index(axis=1), expected.sort_index(axis=1)
        )

    def test_can_create_arbitrary_columns(self):
        data = [
            {
                "eventName": "INSERT",
                "dynamodb": {"NewImage": {"unregistered_column": {"N": "1"}}},
            }
        ]
        expected = pd.DataFrame(
            {"event_name": ["INSERT"], "unregistered_column": ["1"]},
            columns=self.clean_cdc_columns + ["unregistered_column"],
        )
        result = clean_open_platform_cdc(data)
        e_col_set = set(list(expected.columns.values))
        r_col_set = set(list(result.columns.values))
        self.assertFalse(r_col_set.symmetric_difference(e_col_set))
