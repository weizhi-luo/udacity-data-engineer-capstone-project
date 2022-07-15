from unittest import TestCase

from data_validation import are_mappings_valid


class TestAreMappingsValid(TestCase):
    def test_invalid_mappings_false(self):
        mappings = [
            {"latitude": 52.4, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.90594482421875},
            {"latitude": 52.4, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T03:00:00.000000000",
             "t2m": 283.53460693359375}
        ]
        keys = ['2t', 'forecast_date_time', 'value_date_time',
                'latitude', 'longitude']

        valid = are_mappings_valid(mappings, keys)

        self.assertFalse(valid)

    def test_valid_mapping_true(self):
        mappings = [
            {"latitude": 52.4, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T00:00:00.000000000",
             "t2m": 284.90594482421875},
            {"latitude": 52.4, "longitude": -1.2,
             "forecast_date_time": "2022-06-27T00:00:00.000000000",
             "value_date_time": "2022-06-27T03:00:00.000000000",
             "t2m": 283.53460693359375}
        ]
        keys = ['forecast_date_time', 't2m', 'value_date_time',
                'latitude', 'longitude']

        valid = are_mappings_valid(mappings, keys)

        self.assertTrue(valid)
