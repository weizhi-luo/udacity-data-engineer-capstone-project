from unittest import TestCase
from unittest.mock import patch
from os.path import join

from s3_file_data_access import get_mappings_from_file_on_s3


class TestGetMappingsFromFileOnS3(TestCase):
    @patch('s3_file_data_access.download_file_from_s3')
    def test_get_mappings_from_valid_file(self, mock_download_file):
        mock_download_file.return_value = \
            join('data_files', '2022-06-27_00_2t.json')
        expected_mappings = [
            {"latitude": 52.4, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.90594482421875},
            {"latitude": 52.0, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.31219482421875},
            {"latitude": 51.6, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.65594482421875},
            {"latitude": 51.2, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.59344482421875}
        ]

        mappings = get_mappings_from_file_on_s3('test_s3_bucket',
                                                'test_s3_key')

        self.assertEqual(mappings, expected_mappings)
