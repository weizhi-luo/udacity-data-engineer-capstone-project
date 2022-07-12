import json
from os.path import basename

from file_access import get_services_from_file_on_s3, upload_data_to_s3
from data_util import convert_to_service_performances, \
    convert_mappings_to_bytes
from file_tool import create_s3_key_for_file


def lambda_handler(event, context):
    service_performance_file_s3_bucket, service_performance_file_s3_key, \
        destination_s3_bucket, destination_s3_folder = \
        event['service_performance_file_s3_bucket'], \
        event['service_performance_file_s3_key'], \
        event['destination_s3_bucket'], event['destination_s3_folder']

    services = get_services_from_file_on_s3(service_performance_file_s3_bucket,
                                            service_performance_file_s3_key)
    service_performances = [dict(service_performance)
                            for service in services
                            for service_performance
                            in convert_to_service_performances(service)]
    bytes_to_upload = convert_mappings_to_bytes(service_performances)

    destination_s3_key = \
        create_s3_key_for_file(basename(service_performance_file_s3_key),
                               destination_s3_folder)

    upload_data_to_s3(bytes_to_upload, destination_s3_bucket,
                      destination_s3_key)
    return {
        "s3_bucket": destination_s3_bucket,
        "s3_key": destination_s3_key
    }
