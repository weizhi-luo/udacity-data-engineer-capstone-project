from os.path import basename


def create_local_file_path_from_s3_path(s3_key: str) -> str:
    """Create local file path from S3 key

    :param s3_key: File's S3 key
    :type s3_key: str
    :return: File's local path
    :rtype: str
    """
    return '/tmp/' + basename(s3_key)
