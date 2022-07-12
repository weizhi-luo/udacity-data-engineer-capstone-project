import json

from data_model import Area
from file_access import get_xarray_dataset_from_file_on_s3, upload_data_to_s3
from data_conversion import convert_xarray_dataset_with_area_to_mappings, \
    convert_mappings_to_bytes
from file_tool import change_file_extension, get_file_name_without_extension, \
    create_s3_key_for_file


def lambda_handler(event, context):
    ecmwf_file_s3_bucket, ecmwf_file_s3_key, latitude_north, latitude_south,\
        longitude_east, longitude_west, destination_s3_bucket, \
        destination_s3_folder = \
        event['ecmwf_file_s3_bucket'], event['ecmwf_file_s3_key'], \
        event['latitude_north'], event['latitude_south'], \
        event['longitude_east'], event['longitude_west'], \
        event['destination_s3_bucket'], event['destination_s3_folder']

    area = Area(latitude_north, latitude_south, longitude_east, longitude_west)
    dataset = get_xarray_dataset_from_file_on_s3(ecmwf_file_s3_bucket,
                                                 ecmwf_file_s3_key)
    mappings = convert_xarray_dataset_with_area_to_mappings(dataset, area)
    bytes_to_upload = convert_mappings_to_bytes(mappings)

    file_name_without_extension = \
        get_file_name_without_extension(ecmwf_file_s3_key)
    json_file = change_file_extension(file_name_without_extension, 'json')
    destination_s3_key = \
        create_s3_key_for_file(json_file, destination_s3_folder)

    upload_data_to_s3(bytes_to_upload, destination_s3_bucket,
                      destination_s3_key)
    return {
        "s3_bucket": destination_s3_bucket,
        "s3_key": destination_s3_key
    }
