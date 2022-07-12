from os.path import join, basename


def create_local_file_path_from_s3_path(s3_key: str) -> str:
    """Create local file path from S3 key

    :param s3_key: File's S3 key
    :type s3_key: str
    :return: File's local path
    :rtype: str
    """
    return '/tmp/' + basename(s3_key)


def create_s3_key_for_file(file_name: str, s3_folder: str, *args: str) -> str:
    """Create an S3 key for a file

    :param file_name: Name of the file
    :type file_name: str
    :param s3_folder: Folder on S3
    :type s3_folder: str
    :param args: optional folders to be added to the key
    :type args: str

    :return: An S3 key to the file
    :rtype: str
    """
    return join(s3_folder, *args, file_name)
