from datetime import date, datetime, time
from unittest import TestCase
from unittest.mock import patch

import awswrangler as wr
import pandas as pd
import pandas.testing as pdtest
from etl.open_platform_status.handlers.load_open_platform import (
    format_date_key, format_time_key, get_connected, get_disconnected,
    get_export_datetime)


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
        database = "foo"
        table = "bar"
        export_id = "1234-567a"
        summary_df = pd.DataFrame(
            data={
                "export_time": [
                    datetime(2022, 3, 7, 18, 45, 38),
                    datetime(2022, 3, 8, 9, 5, 8),
                ],
                "export_id": ["9999-9999", "1234-567a"],
            }
        )
        expected_date = date(2022, 3, 8)
        expected_time = time(9, 5, 8)

        with patch.object(
            wr.s3, "read_parquet_table", return_value=summary_df
        ) as mock_method:
            res_date, res_time = get_export_datetime(database, table, export_id)
            mock_method.assert_called_once_with(database, table)
            self.assertEqual(res_date, expected_date)
            self.assertEqual(res_time, expected_time)

    def test_get_disconnected(self):
        fact_df = pd.DataFrame(
            {"company_key": ["Kompanie"], "platform": ["Lazada"], "status": [True]}
        )
        incoming_df = pd.DataFrame({"company_key": [], "platform_name": []})
        expected = pd.DataFrame(
            {
                "company_key": ["Kompanie"],
                "platform": ["Lazada"],
            }
        )
        result = get_disconnected(fact_df, incoming_df)
        pdtest.assert_frame_equal(result, expected)

    def test_get_connected(self):
        fact_df = pd.DataFrame(
            {
                "company_key": ["Kompanie", "Kompanie"],
                "platform": ["Lazada", "Shopee"],
                "status": [True, False],
            }
        )
        incoming_df = pd.DataFrame(
            {
                "company_key": ["Kompanie", "Corporate"],
                "platform_name": ["Shopee", "K-Cash"],
            }
        )
        expected = pd.DataFrame(
            {
                "company_key": ["Kompanie", "Corporate"],
                "platform": ["Shopee", "K-Cash"],
            }
        )

        result = get_connected(fact_df, incoming_df)
        pdtest.assert_frame_equal(result, expected)
