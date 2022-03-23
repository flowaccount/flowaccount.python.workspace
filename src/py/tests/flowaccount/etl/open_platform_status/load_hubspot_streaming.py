import json
from datetime import datetime
from unittest import TestCase

import pandas as pd
import pandas.testing as pdtest
from flowaccount.etl.open_platform_status.load_hubspot_streaming import (
    aggregate_latest_status, attach_hubspot_id, convert_to_json_line,
    convert_to_platform_status_dict, filter_event, filter_platform)


class AttachHubSpotIdTestCase(TestCase):
    def test_attach_succeeds(self):
        cdc_df = pd.DataFrame({"company_id": [1]})
        mapping_df = pd.DataFrame({"company_id": [1], "hubspot_id": [1001]})
        expected = pd.DataFrame({"company_id": [1], "hubspot_id": [1001]})
        result, _ = attach_hubspot_id(cdc_df, mapping_df)
        pdtest.assert_frame_equal(result, expected)

    def test_return_no_mapping_succeeds(self):
        cdc_df = pd.DataFrame({"company_id": [1]}).astype({"company_id": "Int64"})
        mapping_df = pd.DataFrame({"company_id": [2], "hubspot_id": [1002]}).astype(
            {"company_id": "Int64", "hubspot_id": "Int64"}
        )
        expected = pd.DataFrame({"company_id": [1], "hubspot_id": [pd.NA]}).astype(
            {"company_id": "Int64", "hubspot_id": "Int64"}
        )
        _, result = attach_hubspot_id(cdc_df, mapping_df)
        pdtest.assert_frame_equal(result, expected)


class FilterPlatformTestCase(TestCase):
    def test_filter_succeeds(self):
        connection_df = pd.DataFrame({"platform_name": ["Lazada", "Grab"]})
        platforms = ["Lazada"]
        expected_pass_df = pd.DataFrame({"platform_name": ["Lazada"]})
        expected_fail_df = pd.DataFrame({"platform_name": ["Grab"]})
        pass_df, fail_df = filter_platform(connection_df, platforms=platforms)
        pdtest.assert_frame_equal(
            pass_df.reset_index(drop=True), expected_pass_df.reset_index(drop=True)
        )
        pdtest.assert_frame_equal(
            fail_df.reset_index(drop=True), expected_fail_df.reset_index(drop=True)
        )


class FilterEventTestCase(TestCase):
    def test_filter_succeeds(self):
        platform_df = pd.DataFrame(
            {"event_name": ["INSERT", "MODIFY", "REMOVE", "ADD"]}
        )
        expected_pass_df = pd.DataFrame({"event_name": ["INSERT", "REMOVE"]})
        expected_fail_df = pd.DataFrame({"event_name": ["MODIFY", "ADD"]})
        pass_df, fail_df = filter_event(platform_df)
        pdtest.assert_frame_equal(
            pass_df.reset_index(drop=True), expected_pass_df.reset_index(drop=True)
        )
        pdtest.assert_frame_equal(
            fail_df.reset_index(drop=True), expected_fail_df.reset_index(drop=True)
        )


class AggregateLatestStatusTestCase(TestCase):
    def test_get_latest_event_succeeds(self):
        event_df = pd.DataFrame(
            {
                "company_id": [1, 1, 1],
                "hubspot_id": [1001, 1001, 1001],
                "approximate_creation_date_time": [
                    datetime(2022, 3, 15),
                    datetime(2022, 3, 1),
                    datetime(2022, 3, 31),
                ],
                "event_name": ["REMOVE", "INSERT", "INSERT"],
                "platform_name": ["Lazada", "Lazada", "Lazada"],
            }
        )
        expected_df = pd.DataFrame(
            {"hubspot_id": [1001], "hubspot_key": ["lazada_api"], "status": ["yes"]}
        ).set_index("hubspot_id")
        result_df = aggregate_latest_status(event_df)
        pdtest.assert_frame_equal(result_df, expected_df)

    def test_handle_multiple_platforms_succeeds(self):
        event_df = pd.DataFrame(
            {
                "company_id": [1, 1],
                "hubspot_id": [1001, 1001],
                "approximate_creation_date_time": [
                    datetime(2022, 3, 1),
                    datetime(2022, 3, 1),
                ],
                "event_name": ["INSERT", "REMOVE"],
                "platform_name": ["Lazada", "Shopee"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "hubspot_id": [1001, 1001],
                "hubspot_key": ["lazada_api", "shopee_api"],
                "status": ["yes", "no"],
            }
        ).set_index("hubspot_id")
        result_df = aggregate_latest_status(event_df)
        pdtest.assert_frame_equal(result_df, expected_df)

    def test_handle_multiple_companies_succeeds(self):
        event_df = pd.DataFrame(
            {
                "company_id": [1, 2],
                "hubspot_id": [1001, 1002],
                "approximate_creation_date_time": [
                    datetime(2022, 3, 1),
                    datetime(2022, 3, 1),
                ],
                "event_name": ["INSERT", "REMOVE"],
                "platform_name": ["Lazada", "Shopee"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "hubspot_id": [1001, 1002],
                "hubspot_key": ["lazada_api", "shopee_api"],
                "status": ["yes", "no"],
            }
        ).set_index("hubspot_id")
        result_df = aggregate_latest_status(event_df)
        pdtest.assert_frame_equal(result_df, expected_df)


class ConvertToPlatformStatusDictTestCase(TestCase):
    def test_convert_dataframe_succeeds(self):
        status_df = pd.DataFrame(
            {"hubspot_key": ["lazada_api", "shopee_api"], "status": ["yes", "no"]}
        )
        expected = {"lazada_api": "yes", "shopee_api": "no"}
        result = convert_to_platform_status_dict(status_df)
        self.assertDictEqual(result, expected)

    def test_convert_series_succeeds(self):
        status_sr = pd.Series(["lazada_api", "yes"], index=["hubspot_key", "status"])
        expected = {"lazada_api": "yes"}
        result = convert_to_platform_status_dict(status_sr)
        self.assertDictEqual(result, expected)


class ConvertToJsonLineTestCase(TestCase):
    def test_convert_succeeds(self):
        inputs = [
            {"id": 1001, "properties": {"lazada_api": "yes"}},
            {"id": 1002, "properties": {"shopee_api": "yes"}},
        ]
        expected = json.dumps(inputs[0]) + "\n" + json.dumps(inputs[1])
        result = convert_to_json_line(inputs)
        self.assertEqual(result, expected)
