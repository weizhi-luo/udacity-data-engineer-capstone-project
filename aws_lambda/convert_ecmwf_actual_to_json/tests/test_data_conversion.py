import json
import xarray
from unittest import TestCase

from data_conversion import convert_xarray_dataset_with_area_to_mappings
from data_model import Area


class TestConvertXarrayDatasetWithAreaToMappings(TestCase):
    def test_dataset_return_expected_mapping(self):
        with open(r'./data/2022-06-19.json') as file:
            mappings_expected = json.load(file)

        with xarray.open_dataset(r'./data/2022-06-19.nc') as dataset_:
            dataset = dataset_.load()
        area = Area(52.5, 50.5, 1, -1)

        mappings = convert_xarray_dataset_with_area_to_mappings(dataset, area)

        self.assertEqual(mappings, mappings_expected)
