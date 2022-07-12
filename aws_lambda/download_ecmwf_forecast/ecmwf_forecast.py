from os import path

from ecmwf.opendata import Client


def download_file(date: str, time: str, forecast_days: int,
                  parameter: str) -> str:
    """Download ECMWF forecast file and return its path

    :param date: Forecast date
    :param time: Forecast time
    :param forecast_days: Days of the forecast
    :param parameter: Forecast parameter
    :return: Path of the downloaded file
    """
    file_path = create_file_path(date, time, parameter)
    request = create_request(date, time, forecast_days, parameter)
    client = Client()

    client.retrieve(request, file_path)
    return file_path


def create_file_path(date: str, time: str, parameter: str) -> str:
    """Create path of the forecast file to be downloaded

    :param date: Forecast date
    :param time: Forecast time
    :param parameter: Forecast parameter
    :return: Path of the file to be downloaded
    """
    return path.join('/tmp', f'{date}_{time}_{parameter}.grib2')


def create_request(date: str, time: str, forecast_days: int,
                   parameter: str) -> dict:
    """Create the request for downloading forecast file

    :param date: Forecast date
    :param time: Forecast time
    :param forecast_days: Days of forecast
    :param parameter: Forecast parameter
    :return: The request used for download
    """
    return {
        'date': date,
        'time': time,
        'step': create_step(forecast_days),
        'type': 'fc',
        'param': parameter
    }


def create_step(forecast_days: int) -> list:
    """Create the step for downloading forecast file

    :param forecast_days: Days of forecast
    :return: A list of steps
    """
    if forecast_days <= 6:
        return [s for s in range(0, forecast_days*24+3, 3)]
    elif forecast_days > 10:
        raise ValueError('Argument forecast_days should be between 0 and 10.')
    else:
        return [s for s in range(0, 147, 3)] + [s for s in range(150, 246, 6)]
