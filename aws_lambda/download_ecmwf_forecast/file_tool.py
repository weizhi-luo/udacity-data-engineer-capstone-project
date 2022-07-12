from os.path import join, basename


def create_s3_key(file_path: str, date: str, time: str, s3_folder: str) -> str:
    """Create S3 key

    :param file_path: Path of the file to be stored
    :param date: Forecast date of the file
    :param time: Forecast time of the file
    :param s3_folder: Folder in S3 bucket
    :return: S3 path to the file
    """
    return join(s3_folder, time, date, basename(file_path))
