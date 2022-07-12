import json
from typing import List
from datetime import datetime, time
from collections.abc import Iterable, Mapping

from data_model import Service, ServicePerformance


def convert_mappings_to_bytes(mappings: Iterable[Mapping]) -> bytes:
    """Convert a collection of Mappings to bytes

    :param mappings: A collection of Mappings
    :return: bytes
    """
    return bytes(
        '\n'.join(json.dumps(mapping) for mapping in mappings).encode('UTF-8'))


def convert_to_service_performances(service: Service) \
        -> List[ServicePerformance]:
    """Convert an instance of `service` to a list of `ServicePerformance`

    :param service: an instance of `Service`
    :return: a list of `ServicePerformance`
    """
    verify_service(service)

    origin_location = service.service_attributes_metrics.origin_location
    destination_location = \
        service.service_attributes_metrics.destination_location
    service_date = datetime.strptime(
        service.service_attributes_metrics.rids[0][:8], '%Y%m%d').date()
    arrival_time = time(int(service.service_attributes_metrics.gbtt_pta[0:2]),
                        int(service.service_attributes_metrics.gbtt_pta[2:4]))
    departure_time = time(
        int(service.service_attributes_metrics.gbtt_ptd[0:2]),
        int(service.service_attributes_metrics.gbtt_ptd[2:4]))
    operator_code = service.service_attributes_metrics.toc_code
    rid = service.service_attributes_metrics.rids[0]

    return [ServicePerformance(origin_location, destination_location,
                               service_date, arrival_time, departure_time,
                               operator_code, rid, int(metric.tolerance_value),
                               int(metric.num_not_tolerance) != 0) for metric
            in service.metrics]


def verify_service(service: Service) -> None:
    """Verify if the service data is valid to process

    :param service: an instance of `Service`
    :return: None
    """
    if len(service.service_attributes_metrics.rids) != 1:
        raise ValueError('The number of rids is not equal to 1.')
