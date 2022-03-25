from datetime import datetime
from unittest import TestCase

import pandas as pd
import pandas.testing as pdtest
from flowaccount.etl.open_platform_status.load_redshift_streaming import (
    convert_to_fact_table, filter_new_company)


class FilterNewCompanyTestCase(TestCase):
    def test_filter_succeeds(self):
        cdc_df = pd.DataFrame({"company_id": [1000, 1001]})
        company_df = pd.DataFrame({"company_key": [1], "company_id": [1000]})
        expected = pd.DataFrame({"company_id": [1001]})
        result = filter_new_company(cdc_df, company_df)
        pdtest.assert_frame_equal(
            result.reset_index(drop=True), expected.reset_index(drop=True)
        )


class ConvertToFactTableTestCase(TestCase):
    def test_convert_succeeds(self):
        cdc_df = pd.DataFrame(
            {
                "approximate_creation_date_time": [
                    datetime(2022, 3, 1, 9, 15, 0),
                    datetime(2022, 3, 15, 14, 30, 0),
                    datetime(2022, 3, 31, 8, 45, 0),
                ],
                "company_id": [1000, 1001, 1002],
                "event_name": ["INSERT", "MODIFY", "REMOVE"],
                "platform_name": ["Lazada", "Shopee", "Shopee"],
            }
        )
        company_df = pd.DataFrame(
            {
                "company_key": [1, 2, 3, 4, 5],
                "company_id": [1000, 1001, 1002, 1003, 1004],
            }
        )
        expected = pd.DataFrame(
            {
                "date_key": [20220301, 20220331],
                "time_key": [91500, 84500],
                "company_key": [1, 3],
                "status": [True, False],
                "platform": ["Lazada", "Shopee"],
            }
        ).astype(
            {
                "date_key": "int",
                "time_key": "int",
                "company_key": "int",
                "status": "boolean",
                "platform": "string",
            }
        )
        result = convert_to_fact_table(cdc_df, company_df)
        pdtest.assert_frame_equal(
            result.reset_index(drop=True).sort_index(axis=1),
            expected.reset_index(drop=True).sort_index(axis=1),
        )
