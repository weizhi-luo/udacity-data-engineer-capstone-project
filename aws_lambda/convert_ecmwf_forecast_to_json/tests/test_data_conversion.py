import json
import xarray
from unittest import TestCase

from data_model import Area
from data_conversion import convert_xarray_dataset_with_area_to_mappings, \
    convert_mappings_to_bytes


class TestConvertXarrayDatasetWithAreaToMappings(TestCase):
    def test_dataset_indexed_by_latitude_longitude_return_expected_mapping(
            self):
        with open(r'./data/2022-06-22_00_2t.json') as file:
            mappings_expected = json.load(file)

        with xarray.open_dataset(r'./data/2022-06-22_00_2t.grib2',
                                 engine='cfgrib') as dataset_:
            dataset = dataset_.load()
        area = Area(52.5, 50.5, 1, -1)

        mappings = convert_xarray_dataset_with_area_to_mappings(dataset, area)

        self.assertEqual(mappings, mappings_expected)

    def test_dataset_indexed_by_latitude_longitude_step_return_expected_mapping(
            self):
        with open(r'./data/2022-06-26_18.json') as file:
            mappings_expected = json.load(file)

        with xarray.open_dataset(r'./data/2022-06-26_18.grib2',
                                 engine='cfgrib') as dataset_:
            dataset = dataset_.load()
        area = Area(52.5, 50.5, 1, -1)

        mappings = convert_xarray_dataset_with_area_to_mappings(dataset, area)

        self.assertEqual(mappings, mappings_expected)


class TestConvertMappingsToBytes(TestCase):
    def test_convert_mappings_to_bytes(self):
        mappings = [
            {
                "key": 52.4,
                "value": 287.78271484375
            },
            {
                "key": 52.0,
                "value": 287.50146484375
            },
            {
                "key": 51.6,
                "value": 287.87646484375
            }
        ]
        expected_bytes = bytes(
            ('{"key": 52.4, "value": 287.78271484375}\n'
             '{"key": 52.0, "value": 287.50146484375}\n'
             '{"key": 51.6, "value": 287.87646484375}').encode('UTF-8')
        )

        converted_bytes = convert_mappings_to_bytes(mappings)

        self.assertEqual(converted_bytes, expected_bytes)
