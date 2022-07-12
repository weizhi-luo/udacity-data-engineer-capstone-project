import os
import boto3


def store_file_to_s3(file_path: str, s3_bucket: str, s3_key: str) -> None:
    """Upload ecmwf forecast file to S3

    :param file_path: Local path of file to be stored
    :param s3_bucket: S3 bucket name
    :param s3_key: S3 key for the file to be stored
    :return: None
    """
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file_path, s3_bucket, s3_key)


def delete_file(file_path: str) -> None:
    """Delete file with path

    :param file_path: path of file to be deleted
    :return: None
    """
    os.remove(file_path)
