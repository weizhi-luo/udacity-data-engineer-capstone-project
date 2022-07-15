import boto3

from file_tool import create_local_file_path_from_s3_path


def download_file_from_s3(s3_bucket, s3_key) -> str:
    """Download file from S3 and return the local file path

    :param s3_bucket: S3 bucket
    :param s3_key: S3 key
    :return: local file path
    """
    local_file_path = create_local_file_path_from_s3_path(s3_key)

    s3_client = boto3.client('s3')
    s3_client.download_file(s3_bucket, s3_key, local_file_path)

    return local_file_path
