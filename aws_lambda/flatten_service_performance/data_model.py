import json

from typing import List
from datetime import date, time


class Metric:
    """Represent Metric in National Rail history service performance data"""

    def __init__(self, tolerance_value: str, num_not_tolerance: str,
                 num_tolerance: str, percent_tolerance: str,
                 global_tolerance: bool):
        """Create an instance of `Metric`

        :param tolerance_value: tolerance minute value
        :param num_not_tolerance: number of services performed over tolerance
        :param num_tolerance: number of services performed within tolerance
        :param percent_tolerance: percentage of services reaching tolerance
        :param global_tolerance: indicate if this tolerance is global
        """
        self._tolerance_value = tolerance_value
        self._num_not_tolerance = num_not_tolerance
        self._num_tolerance = num_tolerance
        self._percent_tolerance = percent_tolerance
        self._global_tolerance = global_tolerance

    @property
    def tolerance_value(self) -> int:
        return int(self._tolerance_value)

    @property
    def num_not_tolerance(self) -> int:
        return int(self._num_not_tolerance)

    @property
    def num_tolerance(self) -> int:
        return int(self._num_tolerance)

    @property
    def percent_tolerance(self) -> float:
        return float(self._percent_tolerance)

    @property
    def global_tolerance(self) -> bool:
        return self._global_tolerance

    def __iter__(self):
        yield from {
            "tolerance_value": self.tolerance_value,
            "num_not_tolerance": self.num_not_tolerance,
            "num_tolerance": self.num_tolerance,
            "percent_tolerance": self.percent_tolerance,
            "global_tolerance": self.global_tolerance
        }.items()

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return self.__str__()

    @staticmethod
    def from_json(json_data: dict):
        return Metric(json_data['tolerance_value'],
                      json_data['num_not_tolerance'],
                      json_data['num_tolerance'],
                      json_data['percent_tolerance'],
                      json_data['global_tolerance'])


class ServiceAttributesMetrics:
    """Represent National rail service's attributes related to its metrics"""

    def __init__(self, origin_location: str, destination_location: str,
                 gbtt_ptd: str, gbtt_pta: str, toc_code: str,
                 matched_services: int, rids: List[str]):
        """Create an instance of `ServiceAttributesMetrics`

        :param origin_location: origin location in National Rail's station code
        :param destination_location: destination location in station code
        :param gbtt_ptd: departure time
        :param gbtt_pta: arrival time
        :param toc_code: operator's code
        :param matched_services: number of matched services
        :param rids: RIDs of matched services
        """
        self._origin_location = origin_location
        self._destination_location = destination_location
        self._gbtt_ptd = gbtt_ptd
        self._gbtt_pta = gbtt_pta
        self._toc_code = toc_code
        self._matched_services = matched_services
        self._rids = rids

    @property
    def origin_location(self):
        return self._origin_location

    @property
    def destination_location(self):
        return self._destination_location

    @property
    def gbtt_ptd(self):
        return self._gbtt_ptd

    @property
    def gbtt_pta(self):
        return self._gbtt_pta

    @property
    def toc_code(self):
        return self._toc_code

    @property
    def matched_services(self):
        return int(self._matched_services)

    @property
    def rids(self):
        return self._rids

    def __iter__(self):
        yield from {
            "origin_location": self.origin_location,
            "destination_location": self.destination_location,
            "gbtt_ptd": self.gbtt_ptd,
            "gbtt_pta": self.gbtt_pta,
            "toc_code": self.toc_code,
            "matched_services": self.matched_services,
            "rids": self.rids
        }.items()

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return self.__str__()

    @staticmethod
    def from_json(json_data: dict):
        return ServiceAttributesMetrics(
            json_data['origin_location'], json_data['destination_location'],
            json_data['gbtt_ptd'], json_data['gbtt_pta'],
            json_data['toc_code'], json_data['matched_services'],
            json_data['rids'])


class Service:
    """Represent a National Rail service by attributes and related metrics"""

    def __init__(self, service_attributes_metrics: ServiceAttributesMetrics,
                 metrics: List[Metric]):
        """Create an instance of `Service`

        :param service_attributes_metrics: attributes of the service by origin,
                                           destination, departure and arrival
                                           times
        :param metrics: a list of metrics related this service based on the
                        minute tolerated for delay
        """
        self._service_attributes_metrics = service_attributes_metrics
        self._metrics = metrics

    @property
    def service_attributes_metrics(self) -> ServiceAttributesMetrics:
        return self._service_attributes_metrics

    @property
    def metrics(self) -> List[Metric]:
        return self._metrics

    def __iter__(self):
        yield from {
            "service_attributes_metrics":
                self.service_attributes_metrics.to_json(),
            "metrics": [m.to_json() for m in self.metrics]
        }.items()

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.__str__()

    def to_json(self):
        return self.__str__()

    @staticmethod
    def from_json(json_data: dict):
        if 'serviceAttributesMetrics' in json_data and 'Metrics' in json_data:
            service_attributes_metrics = ServiceAttributesMetrics.from_json(
                json_data['serviceAttributesMetrics'])
            metrics = [Metric.from_json(m) for m in json_data['Metrics']]
            return Service(service_attributes_metrics, metrics)
        else:
            return json_data


class ServicePerformance:
    """Represent performance based on data from `Service` and `Metric`"""

    def __init__(self, origin_location: str, destination_location: str,
                 service_date: date, arrival_time: time, departure_time: time,
                 operator_code: str, rid: str, delay_tolerance_minute: int,
                 delayed: bool):
        """Create an instance of `ServicePerformance`

        :param origin_location: origin location in National Rail's station code
        :param destination_location: destination location in station code
        :param service_date: train service date
        :param arrival_time: train service arrival time
        :param departure_time: train service departure time
        :param operator_code: operator code
        :param rid: RID of this train service
        :param delay_tolerance_minute: delay tolerance in minute
        :param delayed: this train service is delayed or not
        """
        self._origin_location = origin_location
        self._destination_location = destination_location
        self._service_date = service_date
        self._arrival_time = arrival_time
        self._departure_time = departure_time
        self._operator_code = operator_code
        self._rid = rid
        self._delay_tolerance_minute = delay_tolerance_minute
        self._delayed = delayed

    @property
    def origin_location(self):
        return self._origin_location

    @property
    def destination_location(self):
        return self._destination_location

    @property
    def service_date(self):
        return self._service_date

    @property
    def arrival_time(self):
        return self._arrival_time

    @property
    def departure_time(self):
        return self._departure_time

    @property
    def operator_code(self):
        return self._operator_code

    @property
    def rid(self):
        return self._rid

    @property
    def delay_tolerance_minute(self):
        return self._delay_tolerance_minute

    @property
    def delayed(self):
        return self._delayed

    def __iter__(self):
        yield from {
            "origin_location": self.origin_location,
            "destination_location": self.destination_location,
            "date": self.service_date.strftime("%Y-%m-%d"),
            "arrival_time": self.arrival_time.strftime("%H:%M"),
            "departure_time": self.departure_time.strftime("%H:%M"),
            "operator_code": self.operator_code,
            "rid": self.rid,
            "delay_tolerance_minute": self.delay_tolerance_minute,
            "delayed": self.delayed
        }.items()

    def __str__(self):
        return json.dumps(dict(self), ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, ServicePerformance):
            return self.origin_location == other.origin_location \
                   and self.destination_location \
                   == other.destination_location \
                   and self.service_date == other.service_date \
                   and self.arrival_time == other.arrival_time \
                   and self.departure_time == other.departure_time \
                   and self.operator_code == other.operator_code \
                   and self.rid == other.rid \
                   and self.delay_tolerance_minute \
                   == other.delay_tolerance_minute \
                   and self.delayed == other.delayed
        return False
