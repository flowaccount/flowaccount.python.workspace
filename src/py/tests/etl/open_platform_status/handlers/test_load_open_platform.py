from datetime import date, datetime, time
from unittest import TestCase
from unittest.mock import patch

import awswrangler as wr
import pandas as pd
import pandas.testing as pdtest
from etl.open_platform_status.handlers.load_open_platform import (
    format_date_key, format_time_key, get_export_datetime,
    get_open_platform_from_s3, get_platform_status)


class LoadOpenPlatformTestCase(TestCase):
    def test_format_date_key_succeeds(self):
        data = date(2022, 3, 8)
        expected = 20220308
        result = format_date_key(data)
        self.assertEqual(result, expected)

    def test_format_time_key(self):
        data = time(9, 5, 8)
        expected = 90508
        result = format_time_key(data)
        self.assertEqual(result, expected)

    def test_get_export_datetime(self):
        export_id = "1234-567a"
        s3_key = f"s3://test-bucket/path/to/summary/{export_id}.parquet"
        summary_df = pd.DataFrame(
            data={
                "export_time": [datetime(2022, 3, 8, 9, 5, 8)],
                "export_id": ["1234-567a"],
            }
        )
        expected_date = date(2022, 3, 8)
        expected_time = time(9, 5, 8)

        with patch.object(
            wr.s3, "read_parquet", return_value=summary_df
        ) as mock_method:
            res_date, res_time = get_export_datetime(s3_key)
            mock_method.assert_called_once_with(s3_key)
            self.assertEqual(res_date, expected_date)
            self.assertEqual(res_time, expected_time)

    def test_get_open_platform_from_s3(self):
        export_id = "1234-567a"
        s3_key = f"s3://test-bucket/path/to/open_platform/{export_id}.parquet"
        dataset_df = pd.DataFrame(
            {
                "export_id": ["1234-567a"],
                "company_id": [5],
                "platform_name": ["Lazada"],
            }
        )
        expected = pd.DataFrame(
            {
                "company_id": [5],
                "platform_name": ["Lazada"],
            }
        ).astype({"company_id": "int", "platform_name": "category"})

        with patch.object(
            wr.s3, "read_parquet", return_value=dataset_df
        ) as mock_method:
            result = get_open_platform_from_s3(s3_key)
            mock_method.assert_called_once_with(s3_key)

        pdtest.assert_frame_equal(result, expected)

    def test_get_open_platform_from_s3(self):
        platform_sr = pd.Series(["Lazada", "Shopee"], name="platform")
        company_df = pd.DataFrame({"company_key": [3], "dynamodb_key": [5]})
        s3_df = pd.DataFrame({"company_id": [5], "platform_name": ["Lazada"]})

        expected = pd.DataFrame(
            {
                "company_key": [3, 3],
                "platform": ["Lazada", "Shopee"],
                "status": [True, False],
            }
        )

        result = get_platform_status(company_df, s3_df, platform_sr)

        pdtest.assert_frame_equal(result, expected)
