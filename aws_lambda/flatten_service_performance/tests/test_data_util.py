import json
from os.path import join
from unittest import TestCase
from datetime import date, time

from data_util import convert_to_service_performances
from data_model import Service, ServicePerformance


class TestConvertToServicePerformance(TestCase):
    def test_convert_invalid_data_raise_ValueError(self):
        invalid_data_file = \
            'performance_data_with_more_than_one_matched_service.json'
        with open(join('data', invalid_data_file)) as file:
            services = [Service.from_json(service)
                        for service in json.load(file)['Services']]
        with self.assertRaises(ValueError) as cm:
            [convert_to_service_performances(service)
             for service in services]
        self.assertEqual('The number of rids is not equal to 1.',
                         str(cm.exception))

    def test_convert_valid_data_return_expected_data(self):
        expected_service_performances = [
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(7, 53),
                               time(6, 33), 'SN', '202206098777194', 0, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(7, 53),
                               time(6, 33), 'SN', '202206098777194', 5, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(7, 53),
                               time(6, 33), 'SN', '202206098777194', 10,
                               False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(7, 53),
                               time(6, 33), 'SN', '202206098777194', 15,
                               False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 23),
                               time(7, 3), 'SN', '202206098777196', 0, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 23),
                               time(7, 3), 'SN', '202206098777196', 5, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 23),
                               time(7, 3), 'SN', '202206098777196', 10,
                               False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 23),
                               time(7, 3), 'SN', '202206098777196', 15,
                               False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 53),
                               time(7, 33), 'SN', '202206098777197', 0, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 53),
                               time(7, 33), 'SN', '202206098777197', 5, False),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 53),
                               time(7, 33), 'SN', '202206098777197', 10, True),
            ServicePerformance("UCK", "LBG", date(2022, 6, 9), time(8, 53),
                               time(7, 33), 'SN', '202206098777197', 15, True)
        ]

        valid_data_file = "valid_performance_data.json"
        with open(join('data', valid_data_file)) as file:
            services = [Service.from_json(service)
                        for service in json.load(file)['Services']]
        service_performances = [service_performance
                                for service in services
                                for service_performance
                                in convert_to_service_performances(service)]

        self.assertListEqual(service_performances,
                             expected_service_performances)
