from datetime import datetime
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from operators import AwsRequestResponseLambdaOperator


DAG_ID = 'daily_ecmwf_actual_download'
DOWNLOAD_FILE_TASK_ID = 'download_file'
DOWNLOAD_FILE_XCOM_KEY = 'download_file_path'
UPLOAD_FILE_TASK_ID = 'upload_file'
UPLOAD_FILE_S3_BUCKET_XCOM_KEY = 's3_bucket'
UPLOAD_FILE_S3_KEY_XCOM_KEY = 's3_key'


with DAG(
    dag_id=DAG_ID,
    description='Daily ECMWF actual file download',
    schedule_interval='@daily',
    start_date=datetime(2022, 6, 1, 10, 0, 0),
    catchup=False
) as dag:

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException(
            "Failing task because one or more upstream tasks failed.")

    def clear_downloaded_files(download_path):
        from os import listdir, remove
        from os.path import join, isfile
        import os
        print(os.getcwd())
        for item in listdir(os.getcwd()):
            print(item)

        for item in listdir(download_path):
            if not isfile(join(download_path, item)):
                continue
            remove(join(download_path, item))

    def download_file(era5_variables: str = None, era5_sub_region: str = None,
                      era5_time: str = None, value_date_year: str = None,
                      value_date_month: str = None, value_date_day: str = None,
                      download_path: str = None, ti=None):
        import json
        import cdsapi

        sub_region = json.loads(era5_sub_region)
        download_file_path = \
            create_download_file_path(download_path, value_date_year,
                                      value_date_month, value_date_day)

        c = cdsapi.Client()
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'variable': json.loads(era5_variables),
                'product_type': "reanalysis",
                'year': int(value_date_year),
                'month': int(value_date_month),
                'day': int(value_date_day),
                'time': json.loads(era5_time),
                'area': [sub_region['north'], sub_region['west'],
                         sub_region['south'], sub_region['east']],
                'format': 'netcdf'
            }, download_file_path)
        ti.xcom_push(key=DOWNLOAD_FILE_XCOM_KEY, value=download_file_path)

    def create_download_file_path(download_path: str, year: str, month: str,
                                  day: str):
        from os.path import join
        month = month if len(month) == 2 else f'0{month}'
        day = day if len(day) == 2 else f'0{day}'
        file_name = f'{year}-{month}-{day}.nc'
        return join(download_path, file_name)

    def upload_file(file_path: str = None, aws_conn_id: str = None,
                    s3_bucket: str = None, s3_folder: str = None,
                    s3_region: str = None, ti=None):
        from os.path import join, basename
        s3_key = join(s3_folder, basename(file_path))
        s3_hook = S3Hook(aws_conn_id=aws_conn_id, region_name=s3_region)
        s3_hook.load_file(file_path, key=s3_key, bucket_name=s3_bucket,
                          replace=True)
        ti.xcom_push(key=UPLOAD_FILE_S3_BUCKET_XCOM_KEY, value=s3_bucket)
        ti.xcom_push(key=UPLOAD_FILE_S3_KEY_XCOM_KEY, value=s3_key)


    prepare_download_path_task = PythonOperator(
        task_id='prepare_download_path',
        python_callable=clear_downloaded_files,
        op_args=['{{ var.value.daily_ecmwf_actual_download_path }}']
    )

    download_file_task = PythonOperator(
        task_id=DOWNLOAD_FILE_TASK_ID,
        python_callable=download_file,
        op_kwargs={
            'era5_variables': '{{ var.value.ecmwf_era5_variables }}',
            'era5_sub_region': '{{ var.value.ecmwf_era5_sub_region }}',
            'era5_time': '{{ var.value.ecmwf_era5_time }}',
            'value_date_year':
                '{{ (dag_run.logical_date-macros.timedelta(days=7)).year }}',
            'value_date_month':
                '{{ (dag_run.logical_date-macros.timedelta(days=7)).month }}',
            'value_date_day':
                '{{ (dag_run.logical_date-macros.timedelta(days=7)).day }}',
            'download_path': '{{ var.value.daily_ecmwf_actual_download_path }}'
        },
        do_xcom_push=True
    )

    upload_file_task = PythonOperator(
        task_id=UPLOAD_FILE_TASK_ID,
        python_callable=upload_file,
        op_kwargs={
            'file_path': "{{ ti.xcom_pull("
                         f"task_ids='{DOWNLOAD_FILE_TASK_ID}', "
                         f"key='{DOWNLOAD_FILE_XCOM_KEY}', "
                         f"dag_id='{DAG_ID}') }}}}",
            'aws_conn_id': 'aws_credentials',
            's3_bucket': '{{ var.json.capstone_project_aws.s3.bucket_name }}',
            's3_folder': '{{ var.json.capstone_project_aws.s3.'
                         'ecmwf_weather_actual_netcdf_folder }}',
            's3_region': '{{ var.json.capstone_project_aws.s3.region_name }}'
        },
        do_xcom_push=True
    )

    convert_file_task = AwsRequestResponseLambdaOperator(
        task_id='convert_to_json',
        aws_connection_id='aws_credentials',
        region_name='us-west-2',
        function_name='convert_ecmwf_actual_to_json',
        function_payload={
            "ecmwf_file_s3_bucket":
                f"{{{{ ti.xcom_pull(task_ids='{UPLOAD_FILE_TASK_ID}', "
                f"key='{UPLOAD_FILE_S3_BUCKET_XCOM_KEY}', "
                f"dag_id='{DAG_ID}') }}}}",
            "ecmwf_file_s3_key":
                f"{{{{ ti.xcom_pull(task_ids='{UPLOAD_FILE_TASK_ID}', "
                f"key='{UPLOAD_FILE_S3_KEY_XCOM_KEY}', "
                f"dag_id='{DAG_ID}') }}}}",
            "latitude_north": "{{ var.json.ecmwf_era5_sub_region.north }}",
            "latitude_south": "{{ var.json.ecmwf_era5_sub_region.south }}",
            "longitude_east": "{{ var.json.ecmwf_era5_sub_region.east }}",
            "longitude_west": "{{ var.json.ecmwf_era5_sub_region.west }}",
            "destination_s3_bucket":
                "{{ var.json.capstone_project_aws.s3.bucket_name }}",
            "destination_s3_folder":
                "{{ var.json.capstone_project_aws.s3."
                "ecmwf_weather_actual_json_folder }}"
        },
        do_xcom_push=False
    )

    watcher_task = watcher()

    prepare_download_path_task >> download_file_task
    download_file_task >> upload_file_task
    upload_file_task >> convert_file_task
    convert_file_task >> watcher_task
