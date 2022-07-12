from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='test_dag',
    description='Test DAG',
    start_date=datetime(2022, 6, 1),
    catchup=False
) as dag:
    download_path = Variable.get('download_path', default_var=None)
    ID = '{{ dag_run.dag_id }}'
    # d = json.loads("{{ var.json.ecmwf_era5_variables }}")
    # t = type(d)
    download_file = 'download_file'

    also_run_this = BashOperator(
        task_id='also_run_this',
        bash_command="echo t {{ ti.xcom_pull(task_ids='download_file', dag_id='daily_ecmwf_actual_download', key='download_file_path', include_prior_dates=True) }}"
    )

    # def upload_file(file_path: str, aws_conn_id: str, s3_bucket: str,
    #                 s3_folder: str, s3_region: str):
    #     from os.path import basename, join
    #     s3_hook = S3Hook(aws_conn_id=aws_conn_id, region_name=s3_region)
    #     s3_hook.load_file(file_path, join(s3_folder, basename(file_path)),
    #                       bucket_name=s3_bucket, replace=True)
    #
    #
    # upload_file_task = PythonOperator(
    #     task_id='upload_file',
    #     python_callable=upload_file,
    #     op_args=[
    #         './downloads/daily_ecmwf_actual_download/2022-6-10.grib',
    #         'aws_credentials', 'udacity-dend-capstone-project-weizhi-luo',
    #         'ecmwf-weather-actual/grib', 'us-west-2'
    #     ]
    # )
