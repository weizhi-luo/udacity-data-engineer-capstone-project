import json
from collections.abc import Mapping, Sequence
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook


class AwsRequestResponseLambdaOperator(BaseOperator):
    """Custom operator for invoking RequestResponse AWS lambda function"""

    template_fields: Sequence[str] = ('_function_payload', )

    def __init__(self, aws_connection_id: str = '', region_name: str = '',
                 function_name: str = '', function_payload: Mapping = None,
                 xcom_key: str = None, *args, **kwargs):
        """Create an instance of `AwsRequestResponseLambdaOperator`

        :param aws_connection_id: AWS connection id specified in airflow
        :param region_name: AWS region name
        :param function_name: AWS lambda function name
        :param function_payload: AWS lambda function payload
        :param xcom_key: Key for xcom
        :return: An instance of `AwsRequestResponseLambdaOperator`
        """
        super(AwsRequestResponseLambdaOperator, self).__init__(*args, **kwargs)
        self._aws_connection_id = aws_connection_id
        self._region_name = region_name
        self._function_name = function_name
        self._function_payload = function_payload
        self._xcom_key = xcom_key

    def execute(self, context):
        payload = json.dumps(self._function_payload)
        self.log.info(payload)
        lambda_hook = LambdaHook(aws_conn_id=self._aws_connection_id,
                                 region_name=self._region_name)
        response = lambda_hook.invoke_lambda(function_name=self._function_name,
                                             invocation_type='RequestResponse',
                                             payload=payload)
        self._check_response(response)
        if self.do_xcom_push:
            self._xcom_push(context, response)

    def _check_response(self, response):
        if response['StatusCode'] != 200 or 'FunctionError' in response \
                or 'x-amz-function-error' in response['ResponseMetadata']:
            error_message = self._dictionary_to_string(
                json.load(response['Payload']))
            raise RuntimeError('Error in executing AWS lambda function.\n'
                               + error_message)

    def _dictionary_to_string(self, dictionary: dict) -> str:
        return '\n'.join(f'{k}:{self._dictionary_value_to_string(v)}'
                         for k, v in dictionary.items())

    @staticmethod
    def _dictionary_value_to_string(dictionary_value) -> str:
        if type(dictionary_value).__name__ == 'list':
            return '\n'.join(dictionary_value)
        return dictionary_value

    def _xcom_push(self, context, response) -> None:
        task_instance = context['task_instance']
        task_instance.xcom_push(
            self._xcom_key, json.dumps(json.load(response['Payload'])))
