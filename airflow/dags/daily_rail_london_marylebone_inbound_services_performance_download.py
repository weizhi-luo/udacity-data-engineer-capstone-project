from datetime import datetime
from airflow.models import DAG, Variable
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from operators import JsonFormatCheckOperator, AwsRequestResponseLambdaOperator


VARIABLE_KEY = 'rail_london_marylebone_inbound_services'
HTTP_CONN_ID = 'national_rail_historical_service_performance'


with DAG(
    dag_id='daily_rail_london_marylebone_inbound_'
           'services_performance_download',
    description='Daily national rail London Marylebone inbound '
                'services performance download',
    schedule_interval='0 0 * * 1-5',
    start_date=datetime(2022, 6, 1),
    catchup=False
) as dag:

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException(
            "Failing task because one or more upstream tasks failed.")

    def create_request_data(service: dict):
        import json
        return json.dumps({
            "from_loc": service["origin_station_crs_code"],
            "to_loc": service["destination_station_crs_code"],
            "from_time": service["from_time"],
            "to_time": service["to_time"],
            "from_date": "{{ ds }}",
            "to_date": "{{ ds }}",
            "days": service["days"],
            "tolerance": service["tolerance"]})

    watcher_task = watcher()

    rail_services = Variable.get(VARIABLE_KEY, deserialize_json=True,
                                 default_var=None)

    for rail_service in rail_services:
        download_task_id = \
            (f'download_{rail_service["origin_station"].replace(" ", "_")}_'
             f'to_{rail_service["destination_station"].replace(" ", "_")}')

        service_performance_download_task = SimpleHttpOperator(
            task_id=download_task_id,
            http_conn_id=HTTP_CONN_ID,
            method='POST',
            data=create_request_data(rail_service),
            headers={"Content-Type": "application/json"},
            do_xcom_push=True,
            response_check=lambda response: response.status_code == 200)

        data_quality_check_task_id = \
            (f'data_quality_check_'
             f'{rail_service["origin_station"].replace(" ", "_")}_'
             f'to_{rail_service["destination_station"].replace(" ", "_")}')
        data_quality_check_task = JsonFormatCheckOperator(
            task_id=data_quality_check_task_id,
            data=f"{{{{ task_instance.xcom_pull('{download_task_id}') }}}}",
            do_xcom_push=False)

        s3_upload_task_id = \
            (f'upload_{rail_service["origin_station"].replace(" ", "_")}_'
             f'to_{rail_service["destination_station"].replace(" ", "_")}')
        s3_upload_task = S3CreateObjectOperator(
            task_id=s3_upload_task_id,
            s3_bucket='udacity-dend-capstone-project-weizhi-luo',
            s3_key=('rail-service-performance/{{ ds }}_'
                    f'{rail_service["origin_station"]}_to_'
                    f'{rail_service["destination_station"]}.json'),
            data=f"{{{{ task_instance.xcom_pull('{download_task_id}') }}}}",
            replace=True, aws_conn_id='aws_credentials')

        flatten_task_id = \
            (f'flatten_{rail_service["origin_station"].replace(" ", "_")}_'
             f'to_{rail_service["destination_station"].replace(" ", "_")}')
        flatten_file_task = AwsRequestResponseLambdaOperator(
            task_id=flatten_task_id,
            aws_connection_id='aws_credentials',
            region_name='us-west-2',
            function_name='flatten_service_performance',
            function_payload={
                "service_performance_file_s3_bucket":
                    "udacity-dend-capstone-project-weizhi-luo",
                "service_performance_file_s3_key":
                    ('rail-service-performance/{{ ds }}_'
                     f'{rail_service["origin_station"]}_to_'
                     f'{rail_service["destination_station"]}.json'),
                "destination_s3_bucket":
                    "udacity-dend-capstone-project-weizhi-luo",
                "destination_s3_folder": "rail-service-performance/flatten"
            },
            xcom_key='return_value',
            do_xcom_push=True
        )

        service_performance_download_task >> data_quality_check_task
        data_quality_check_task >> s3_upload_task
        s3_upload_task >> flatten_file_task
        flatten_file_task >> watcher_task
