import json
from collections.abc import Sequence
from airflow.models.baseoperator import BaseOperator


class JsonFormatCheckOperator(BaseOperator):
    """Custom operator for checking the data is in json format"""

    template_fields: Sequence[str] = ('_data',)

    def __init__(self, data: str, *args, **kwargs):
        """Create an instance of `JsonFormatCheckOperator`

        :param data: data to be checked
        :return: An instance of `JsonFormatCheckOperator`
        """
        super(JsonFormatCheckOperator, self).__init__(*args, **kwargs)
        self._data = data

    def execute(self, context):
        json.loads(self._data)
