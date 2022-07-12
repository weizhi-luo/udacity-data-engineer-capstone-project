from ecmwf_forecast import download_file
from file_tool import create_s3_key
from file_repository import store_file_to_s3, delete_file


def lambda_handler(event, context):
    date, time, forecast_days, parameter, s3_bucket, s3_folder = \
        event['date'], event['time'], event['forecast_days'], \
        event['parameter'], event['s3_bucket'], event['s3_folder']

    local_file_path = download_file(date, time, forecast_days, parameter)
    s3_key = create_s3_key(local_file_path, date, time, s3_folder)
    store_file_to_s3(local_file_path, s3_bucket, s3_key)
    delete_file(local_file_path)

    return {
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "date": date,
        "time": time,
        'forecast_days': forecast_days,
        "parameter": parameter
    }
