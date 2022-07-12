from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CopyToRedshiftFromS3JsonOperator(BaseOperator):
    """Custom operator for copying data to Redshift data warehouse"""

    def __init__(self, redshift_connection_id: str = '',
                 aws_credentials_id: str = '', table: str = '',
                 s3_bucket: str = '', s3_key: str = '',
                 json_copy_option='auto', *args, **kwargs):
        """Create an instance of `CopyToRedshiftFromS3CsvOperator`

        :param redshift_connection_id: Connection id to AWS Redshift
        :param aws_credentials_id: Credential id to AWS
        :param table: Table on Redshift that data is copied to
        :param s3_bucket: AWS S3 bucket where data is in
        :param s3_key: AWS S3 key pointing to the data
        :param json_copy_option: JSON copy option, the default value is 'auto'
        :return: An instance of `CopyToRedshiftFromS3CsvOperator`
        """
        super(CopyToRedshiftFromS3JsonOperator, self) \
            .__init__(*args, **kwargs)
        self._redshift_connection_id = redshift_connection_id
        self._aws_credentials_id = aws_credentials_id
        self._table = table
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        self._json_copy_option = json_copy_option

    def execute(self, context):
        aws_hook = AwsBaseHook(self._aws_credentials_id, client_type='aws')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self._redshift_connection_id)

        redshift.run(f'TRUNCATE TABLE {self._table}')
        s3_path = f's3://{self._s3_bucket}/{self._s3_key}'
        copy_sql = \
            f"""COPY {self._table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                JSON '{self._json_copy_option}' compupdate off
            """
        redshift.run(copy_sql)


class CopyToRedshiftFromS3JsonManifestOperator(
        CopyToRedshiftFromS3JsonOperator):
    """Custom operator for copying data to Redshift using manifest file"""

    def __init__(self, redshift_connection_id: str = '',
                 aws_credentials_id: str = '', table: str = '',
                 s3_bucket: str = '', s3_key: str = '',
                 json_copy_option='auto', *args, **kwargs):
        """Create an instance of `CopyToRedshiftFromS3JsonManifestOperator`

        :param redshift_connection_id: Connection id to AWS Redshift
        :param aws_credentials_id: Credential id to AWS
        :param table: Table on Redshift that data is copied to
        :param s3_bucket: AWS S3 bucket where data is in
        :param s3_key: AWS S3 key to the manifest file
        :param json_copy_option: JSON copy option, the default value is 'auto'
        :return: An instance of `CopyToRedshiftFromS3JsonManifestOperator`
        """
        super(CopyToRedshiftFromS3JsonManifestOperator, self)\
            .__init__(redshift_connection_id=redshift_connection_id,
                      aws_credentials_id=aws_credentials_id, table=table,
                      s3_bucket=s3_bucket, s3_key=s3_key,
                      json_copy_option=json_copy_option, *args, **kwargs)

    def execute(self, context):
        aws_hook = AwsBaseHook(self._aws_credentials_id, client_type='aws')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self._redshift_connection_id)

        redshift.run(f'TRUNCATE TABLE {self._table}')
        s3_path = f's3://{self._s3_bucket}/{self._s3_key}'
        copy_sql = \
            f"""COPY {self._table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                JSON '{self._json_copy_option}'
                MANIFEST
                compupdate off
            """
        redshift.run(copy_sql)


class CopyToRedshiftFromS3CsvOperator(BaseOperator):
    """Custom operator for copying data to Redshift using csv file"""

    def __init__(self, redshift_connection_id: str = '',
                 aws_credentials_id: str = '', table: str = '',
                 s3_bucket: str = '', s3_key: str = '',
                 ignore_header: int = 0, *args, **kwargs):
        """Create an instance of `CopyToRedshiftFromS3CsvOperator`

        :param redshift_connection_id: Connection id to AWS Redshift
        :param aws_credentials_id: Credential id to AWS
        :param table: Table on Redshift that data is copied to
        :param s3_bucket: AWS S3 bucket where data is in
        :param s3_key: AWS S3 key to the csv file
        :param ignore_header: JSON copy option, the default value is 'auto'
        :return: An instance of `CopyToRedshiftFromS3CsvOperator`
        """
        super(CopyToRedshiftFromS3CsvOperator, self).__init__(*args, **kwargs)
        self._redshift_connection_id = redshift_connection_id
        self._aws_credentials_id = aws_credentials_id
        self._table = table
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        self._ignore_header = ignore_header

    def execute(self, context):
        aws_hook = AwsBaseHook(self._aws_credentials_id, client_type='aws')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self._redshift_connection_id)

        redshift.run(f'TRUNCATE TABLE {self._table}')
        s3_path = f's3://{self._s3_bucket}/{self._s3_key}'
        copy_sql = \
            f"""COPY {self._table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                CSV
                IGNOREHEADER {self._ignore_header}
                compupdate off
            """
        redshift.run(copy_sql)


class RunSqlOnRedshift(BaseOperator):
    """Custom operator for running SQL scripts on Redshift"""

    def __init__(self, redshift_connection_id: str = '',
                 sql_script: str = '', *args, **kwargs):
        """Create an instance of `StageToRedshiftFromS3JsonManifestOperator`

        :param redshift_connection_id: Connection id to AWS Redshift
        :param sql_script: SQL script to be run
        :return: An instance of `RunSqlOnRedshift`
        """
        super(RunSqlOnRedshift, self).__init__(*args, **kwargs)
        self._redshift_connection_id = redshift_connection_id
        self._sqL_script = sql_script

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self._redshift_connection_id)
        redshift.run(self._sqL_script)
