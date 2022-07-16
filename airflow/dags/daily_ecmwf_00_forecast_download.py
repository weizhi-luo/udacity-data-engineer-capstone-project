from datetime import datetime
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from operators import AwsRequestResponseLambdaOperator


DAG_ID = 'daily_ecmwf_00_forecast_download'
CONVERT_FILE_TASK_ID = 'convert_ecmwf_forecast_to_json'
CONVERT_FILE_XCOM_KEY = 'return_value'
JSON_FILE_VALIDATION_PAYLOAD_S3_BUCKET_XCOM_KEY = 's3_bucket'
JSON_FILE_VALIDATION_PAYLOAD_S3_KEY_XCOM_KEY = 's3_key'
JSON_FILE_VALIDATION_PAYLOAD_EXPECTED_KEY_XCOM_KEY = 'expected_keys'
JSON_FILE_VALIDATION_PAYLOAD_CREATION_TASK_ID = \
    'create_json_file_validation_payload'


@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException(
        "Failing task because one or more upstream tasks failed.")


def create_conversion_payload(download_task_result: str, ti):
    import json
    download_task_result_dict = json.loads(download_task_result)
    ti.xcom_push(
        'ecmwf_file_s3_bucket', download_task_result_dict['s3_bucket'])
    ti.xcom_push('ecmwf_file_s3_key', download_task_result_dict['s3_key'])
    ti.xcom_push('date', download_task_result_dict['date'])
    ti.xcom_push('time', download_task_result_dict['time'])


def create_json_file_validation_payload(
        file_conversion_task_result: str = None,
        expected_keys: str = None, ti=None):
    import json
    task_result_dict = json.loads(file_conversion_task_result)
    ti.xcom_push(JSON_FILE_VALIDATION_PAYLOAD_S3_BUCKET_XCOM_KEY,
                 task_result_dict['s3_bucket'])
    ti.xcom_push(JSON_FILE_VALIDATION_PAYLOAD_S3_KEY_XCOM_KEY,
                 task_result_dict['s3_key'])
    ti.xcom_push(JSON_FILE_VALIDATION_PAYLOAD_EXPECTED_KEY_XCOM_KEY,
                 expected_keys)


with DAG(
    dag_id=DAG_ID,
    description='Daily ECMWF forecast 00 file download',
    schedule_interval='0 10 * * *',
    start_date=datetime(2022, 6, 1, 10, 0, 0),
    catchup=False
) as dag:
    parameters = Variable.get("ecmwf_forecast_parameters",
                              deserialize_json=True, default_var=None)
    parameters_expected_keys_in_json = \
        Variable.get("ecmwf_forecast_parameters_expected_keys_in_json",
                     deserialize_json=True, default_var=None)

    download_tasks_dict = {
        parameter: AwsRequestResponseLambdaOperator(
            task_id=f'download_ecmwf_forecast_{parameter}',
            aws_connection_id='aws_credentials',
            region_name='us-west-2',
            function_name='download_ecmwf_forecast',
            function_payload={
                "date": "{{ data_interval_end | ds }}",
                "time": "00",
                "forecast_days": 2,
                "parameter": parameter,
                "s3_bucket": "{{ var.json.capstone_project_aws.s3."
                             "bucket_name }}",
                "s3_folder": "{{ var.json.capstone_project_aws.s3."
                             "ecmwf_weather_forecast_grib2_folder }}"},
            xcom_key=parameter,
            do_xcom_push=True)
        for parameter in parameters
    }

    create_conversion_payload_tasks_dict = {
        parameter: PythonOperator(
            task_id=f'create_conversion_payload_ecmwf_forecast_{parameter}',
            python_callable=create_conversion_payload,
            provide_context=True,
            op_args=["{{ ti.xcom_pull("
                     f"task_ids='download_ecmwf_forecast_{parameter}', "
                     f"key='{parameter}', dag_id='{DAG_ID}') }}}}"])
        for parameter in parameters
    }

    conversion_tasks_dict = {
        parameter: AwsRequestResponseLambdaOperator(
            task_id=f'{CONVERT_FILE_TASK_ID}_{parameter}',
            aws_connection_id='aws_credentials',
            region_name='us-west-2',
            function_name='convert_ecmwf_forecast_to_json',
            function_payload={
                "ecmwf_file_s3_bucket":
                    "{{ ti.xcom_pull(task_ids="
                    f"'create_conversion_payload_ecmwf_forecast_{parameter}', "
                    f"key='ecmwf_file_s3_bucket', dag_id='{DAG_ID}') }}}}",
                "ecmwf_file_s3_key":
                    "{{ ti.xcom_pull(task_ids="
                    f"'create_conversion_payload_ecmwf_forecast_{parameter}', "
                    f"key='ecmwf_file_s3_key', dag_id='{DAG_ID}') }}}}",
                "engine": "cfgrib",
                "date":
                    "{{ ti.xcom_pull(task_ids="
                    f"'create_conversion_payload_ecmwf_forecast_{parameter}', "
                    f"key='date', dag_id='{DAG_ID}') }}}}",
                "time":
                    "{{ ti.xcom_pull(task_ids="
                    f"'create_conversion_payload_ecmwf_forecast_{parameter}', "
                    f"key='time', dag_id='{DAG_ID}') }}}}",
                "latitude_north": "{{ var.json.ecmwf_era5_sub_region.north }}",
                "latitude_south": "{{ var.json.ecmwf_era5_sub_region.south }}",
                "longitude_east": "{{ var.json.ecmwf_era5_sub_region.east }}",
                "longitude_west": "{{ var.json.ecmwf_era5_sub_region.west }}",
                "destination_s3_bucket":
                    "{{ var.json.capstone_project_aws.s3.bucket_name }}",
                "destination_s3_folder":
                    "{{ var.json.capstone_project_aws.s3."
                    "ecmwf_weather_forecast_json_folder }}"
            },
            xcom_key=CONVERT_FILE_XCOM_KEY,
            do_xcom_push=True
        )
        for parameter in parameters
    }

    validation_payload_creation_tasks_dict = {
        parameter: PythonOperator(
            task_id=f'{JSON_FILE_VALIDATION_PAYLOAD_CREATION_TASK_ID}_'
                    f'{parameter}',
            python_callable=create_json_file_validation_payload,
            op_kwargs={
                "file_conversion_task_result":
                    "{{ ti.xcom_pull("
                    f"task_ids='{CONVERT_FILE_TASK_ID}_{parameter}', "
                    f"key='{CONVERT_FILE_XCOM_KEY}', dag_id='{DAG_ID}') }}}}",
                "expected_keys": parameters_expected_keys_in_json[parameter]
            },
            do_xcom_push=True
        )
        for parameter in parameters
    }

    validate_json_file_tasks_dict = {
        parameter: AwsRequestResponseLambdaOperator(
            task_id=f'validate_json_file_{parameter}',
            aws_connection_id='aws_credentials',
            region_name='us-west-2',
            function_name='redshift_json_file_format_check',
            function_payload={
                "s3_bucket":
                    "{{ ti.xcom_pull(task_ids="
                    f"'{JSON_FILE_VALIDATION_PAYLOAD_CREATION_TASK_ID}"
                    f"_{parameter}', "
                    f"key='{JSON_FILE_VALIDATION_PAYLOAD_S3_BUCKET_XCOM_KEY}',"
                    f"dag_id='{DAG_ID}') }}}}",
                "s3_key":
                    "{{ ti.xcom_pull(task_ids="
                    f"'{JSON_FILE_VALIDATION_PAYLOAD_CREATION_TASK_ID}"
                    f"_{parameter}', "
                    f"key='{JSON_FILE_VALIDATION_PAYLOAD_S3_KEY_XCOM_KEY}', "
                    f"dag_id='{DAG_ID}') }}}}",
                "expected_keys":
                    "{{ ti.xcom_pull(task_ids="
                    f"'{JSON_FILE_VALIDATION_PAYLOAD_CREATION_TASK_ID}"
                    f"_{parameter}', "
                    "key="
                    f"'{JSON_FILE_VALIDATION_PAYLOAD_EXPECTED_KEY_XCOM_KEY}', "
                    f"dag_id='{DAG_ID}') }}}}"
            },
            do_xcom_push=False
        )
        for parameter in parameters
    }

    watcher_task = watcher()

    for parameter in parameters:
        download_tasks_dict[parameter] >> \
            create_conversion_payload_tasks_dict[parameter]
        create_conversion_payload_tasks_dict[parameter] >> \
            conversion_tasks_dict[parameter]
        conversion_tasks_dict[parameter] >> \
            validation_payload_creation_tasks_dict[parameter]
        validation_payload_creation_tasks_dict[parameter] >> \
            validate_json_file_tasks_dict[parameter]
        validate_json_file_tasks_dict[parameter] >> watcher_task
