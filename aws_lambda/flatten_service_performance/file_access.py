import os
import boto3
import json
from typing import List

from data_model import Service
from file_tool import create_local_file_path_from_s3_path


def get_services_from_file_on_s3(s3_bucket: str, s3_key: str) -> List[Service]:
    """Get a list of Service from a file on s3

    :param s3_bucket: S3 bucket containing the file
    :param s3_key: S3 key to the file
    :return: A list of instances of `Service`
    """
    local_file_path = create_local_file_path_from_s3_path(s3_key)
    download_file_from_s3(s3_bucket, s3_key, local_file_path)

    with open(local_file_path) as file:
        json_data_collection = json.load(file)['Services']
    services = [Service.from_json(json_datum)
                for json_datum in json_data_collection]

    os.remove(local_file_path)

    return services


def download_file_from_s3(s3_bucket, s3_key, local_file_path):
    s3_client = boto3.client('s3')
    s3_client.download_file(s3_bucket, s3_key, local_file_path)


def upload_data_to_s3(data: bytes, s3_bucket: str, s3_key: str):
    """Upload data to S3 bucket

    :param data: data to be uploaded
    :type data: bytes
    :param s3_bucket: S3 bucket to be uploaded to
    :type s3_bucket: str
    :param s3_key: S3 key to be uploaded to
    :type s3_key: str
    :return: None
    :rtype: None
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=data, Bucket=s3_bucket, Key=s3_key)
