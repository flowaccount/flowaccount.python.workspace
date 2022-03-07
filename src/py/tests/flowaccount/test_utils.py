from unittest import TestCase

from flowaccount.utils import format_snake_case


class FormatSnakeCaseTestCase(TestCase):
    def test_format_from_camel_case_succeeds(self):
        data = "manifestFilesS3Key"
        expected = "manifest_files_s3_key"
        result = format_snake_case(data)
        self.assertEqual(result, expected)
