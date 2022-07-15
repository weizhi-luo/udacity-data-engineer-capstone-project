import json
from typing import List
from collections.abc import Mapping

from file_access import download_file_from_s3


def get_mappings_from_file_on_s3(s3_bucket: str, s3_key: str) -> List[Mapping]:
    """Get a list of Mapping instances from a file on s3

    :param s3_bucket: S3 bucket containing the file
    :param s3_key: S3 key to the file
    :return: A list of instances of `Mapping`
    """
    local_file_path = download_file_from_s3(s3_bucket, s3_key)
    return get_mappings_from_file(local_file_path)


def get_mappings_from_file(file_path: str):
    with open(file_path) as file:
        return [json.loads(line) for line in file]
