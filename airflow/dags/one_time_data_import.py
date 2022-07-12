from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator

from helpers import LoadDimensionTables, LoadFactTables
from operators import CopyToRedshiftFromS3JsonOperator, \
    CopyToRedshiftFromS3JsonManifestOperator, CopyToRedshiftFromS3CsvOperator,\
    RunSqlOnRedshift


REDSHIFT_CONNECTION_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'
S3_BUCKET = 'udacity-dend-capstone-project-weizhi-luo'


with DAG(
    dag_id='one_time_data_import',
    description='One time data import to AWS redshift data warehouse',
    schedule_interval='@once',
    start_date=datetime(2022, 6, 1)
) as dag:
    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException(
            "Failing task because one or more upstream tasks failed.")

    watcher_task = watcher()

    stage_rail_service_performance = \
        CopyToRedshiftFromS3JsonOperator(
            task_id='copy_staging_rail_service_performance',
            redshift_connection_id=REDSHIFT_CONNECTION_ID,
            aws_credentials_id=AWS_CREDENTIALS_ID,
            table='stg.rail_service_performance',
            s3_bucket=S3_BUCKET, s3_key='rail-service-performance/flatten/')

    stage_ecmwf_actual = CopyToRedshiftFromS3JsonOperator(
        task_id='copy_staging_ecmwf_actual',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID, table='stg.ecmwf_actual',
        s3_bucket=S3_BUCKET, s3_key='ecmwf-weather-actual/json/')

    with TaskGroup(group_id='copy_staging_ecmwf_forecast_00') \
            as stage_ecmwf_forecast_00_tg:
        dummy_start = EmptyOperator(task_id='copy_ecmwf_forecast_00_start')

        stage_ecmwf_forecast_00_2t_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_2t',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_2t', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_2t.manifest')

        stage_ecmwf_forecast_00_10u_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_10u',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_10u', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_10u.manifest')

        stage_ecmwf_forecast_00_10v_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_10v',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_10v', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_10v.manifest')

        stage_ecmwf_forecast_00_msl_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_msl',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_msl', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_msl.manifest')

        stage_ecmwf_forecast_00_skt_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_skt',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_skt', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_skt.manifest')

        stage_ecmwf_forecast_00_sp_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_sp',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_sp', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_sp.manifest')

        stage_ecmwf_forecast_00_tcwv_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_tcwv',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_tcwv', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_tcwv.manifest'
            )

        stage_ecmwf_forecast_00_tp_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_00_tp',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_00_tp', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_00_tp.manifest')

        dummy_end = EmptyOperator(task_id='copy_ecmwf_forecast_00_end',
                                  trigger_rule=TriggerRule.ALL_SUCCESS)

        dummy_start >> stage_ecmwf_forecast_00_2t_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_10u_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_10v_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_msl_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_skt_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_sp_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_tcwv_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_00_tp_redshift >> dummy_end

    with TaskGroup(group_id='copy_staging_ecmwf_forecast_12') \
            as stage_ecmwf_forecast_12_tg:
        dummy_start = EmptyOperator(task_id='copy_ecmwf_forecast_12_start')

        stage_ecmwf_forecast_12_2t_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_2t',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_2t', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_2t.manifest')

        stage_ecmwf_forecast_12_10u_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_10u',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_10u', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_10u.manifest')

        stage_ecmwf_forecast_12_10v_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_10v',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_10v', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_10v.manifest')

        stage_ecmwf_forecast_12_msl_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_msl',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_msl', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_msl.manifest')

        stage_ecmwf_forecast_12_skt_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_skt',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_skt', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_skt.manifest')

        stage_ecmwf_forecast_12_sp_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_sp',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_sp', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_sp.manifest')

        stage_ecmwf_forecast_12_tcwv_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_tcwv',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_tcwv', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_tcwv.manifest')

        stage_ecmwf_forecast_12_tp_redshift = \
            CopyToRedshiftFromS3JsonManifestOperator(
                task_id='copy_ecmwf_forecast_12_tp',
                redshift_connection_id=REDSHIFT_CONNECTION_ID,
                aws_credentials_id=AWS_CREDENTIALS_ID,
                table='stg.ecmwf_forecast_12_tp', s3_bucket=S3_BUCKET,
                s3_key='ecmwf-weather-forecast/ecmwf_forecast_12_tp.manifest')

        dummy_end = EmptyOperator(task_id='copy_ecmwf_forecast_12_end',
                                  trigger_rule=TriggerRule.ALL_SUCCESS)

        dummy_start >> stage_ecmwf_forecast_12_2t_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_10u_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_10v_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_msl_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_skt_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_sp_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_tcwv_redshift >> dummy_end
        dummy_start >> stage_ecmwf_forecast_12_tp_redshift >> dummy_end

    copy_staging_end = EmptyOperator(task_id='copy_staging_end',
                                     trigger_rule=TriggerRule.ALL_SUCCESS)

    load_dimension_date = RunSqlOnRedshift(
        task_id='load_dimension_date',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadDimensionTables.LOAD_DATE
    )

    load_dimension_date_time = RunSqlOnRedshift(
        task_id='load_dimension_date_time',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadDimensionTables.LOAD_DATE_TIME
    )

    load_dimension_ecmwf_actual_coordinates = RunSqlOnRedshift(
        task_id='load_dimension_ecmwf_actual_coordinates',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadDimensionTables.LOAD_ECMWF_ACTUAL_COORDINATES
    )

    load_dimension_ecmwf_forecast_coordinates = RunSqlOnRedshift(
        task_id='load_dimension_ecmwf_forecast_coordinates',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadDimensionTables.LOAD_ECMWF_FORECAST_COORDINATES
    )

    load_dimension_train_service_operator = RunSqlOnRedshift(
        task_id='load_dimension_train_service_operator',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadDimensionTables.LOAD_TRAIN_SERVICE_OPERATOR
    )

    copy_dimension_station_codes = CopyToRedshiftFromS3CsvOperator(
        task_id='copy_dimension_station_codes',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID, table='dms.station',
        s3_bucket=S3_BUCKET, s3_key='station_codes.csv', ignore_header=1
    )

    load_dimension_end = EmptyOperator(task_id='load_dimension_end',
                                       trigger_rule=TriggerRule.ALL_SUCCESS)

    load_fact_ecmwf_actual = RunSqlOnRedshift(
        task_id='load_fact_ecmwf_actual',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadFactTables.LOAD_ECMWF_ACTUAL
    )

    load_fact_ecmwf_forecast = RunSqlOnRedshift(
        task_id='load_fact_ecmwf_forecast',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadFactTables.LOAD_ECMWF_FORECAST
    )

    load_fact_rail_service_performance = RunSqlOnRedshift(
        task_id='load_fact_rail_service_performance',
        redshift_connection_id=REDSHIFT_CONNECTION_ID,
        sql_script=LoadFactTables.LOAD_RAIL_SERVICE_PERFORMANCE
    )

    stage_rail_service_performance >> copy_staging_end
    stage_ecmwf_actual >> copy_staging_end
    stage_ecmwf_forecast_00_tg >> copy_staging_end
    stage_ecmwf_forecast_12_tg >> copy_staging_end

    copy_staging_end >> load_dimension_date
    copy_staging_end >> load_dimension_date_time
    copy_staging_end >> load_dimension_ecmwf_actual_coordinates
    copy_staging_end >> load_dimension_ecmwf_forecast_coordinates
    copy_staging_end >> load_dimension_train_service_operator
    copy_staging_end >> copy_dimension_station_codes

    load_dimension_date >> load_dimension_end
    load_dimension_date_time >> load_dimension_end
    load_dimension_ecmwf_actual_coordinates >> load_dimension_end
    load_dimension_ecmwf_forecast_coordinates >> load_dimension_end
    load_dimension_train_service_operator >> load_dimension_end
    copy_dimension_station_codes >> load_dimension_end

    load_dimension_end >> load_fact_ecmwf_actual
    load_dimension_end >> load_fact_ecmwf_forecast
    load_dimension_end >> load_fact_rail_service_performance

    load_fact_ecmwf_actual >> watcher_task
    load_fact_ecmwf_forecast >> watcher_task
    load_fact_rail_service_performance >> watcher_task
