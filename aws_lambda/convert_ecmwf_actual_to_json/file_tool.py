from os.path import join, basename, splitext


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


def get_file_name_without_extension(file_path: str) -> str:
    """Get file name from file path

    :param file_path: File path
    :type file_path: str
    :return: File name
    :rtype: str
    """
    return splitext(basename(file_path))[0]


def change_file_extension(file_name: str, file_extension: str) -> str:
    """Change file extension

    :param file_name: Name of the file, it can contain extension
    :type file_name: str
    :param file_extension: File extension
    :type file_extension: str
    :return: File name with extension intended
    :rtype: str
    """
    return f'{splitext(file_name)[0]}.{file_extension}'


def create_local_file_path_from_s3_path(s3_key: str) -> str:
    """Create local file path from S3 key

    :param s3_key: File's S3 key
    :type s3_key: str
    :return: File's local path
    :rtype: str
    """
    return join('/tmp', basename(s3_key))
