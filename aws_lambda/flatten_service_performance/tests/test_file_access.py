from unittest import TestCase
from unittest.mock import patch, mock_open

from file_access import upload_data_to_s3, get_services_from_file_on_s3


class TestUploadDataToS3(TestCase):
    @patch('botocore.client.BaseClient._make_api_call')
    def test_s3_bucket_s3_key_are_called(self, mock_boto3_client_object):
        data_to_upload = bytes('{"test_key": "test_value"}'.encode('UTF-8'))
        s3_bucket = 'test_bucket'
        s3_key = 'test_folder/test_file.json'

        upload_data_to_s3(data_to_upload, s3_bucket, s3_key)

        mock_boto3_client_object.assert_called_with(
            'PutObject', {'Body': data_to_upload, 'Bucket': s3_bucket,
                          'Key': s3_key})


class TestGetServicesFromFileOnS3(TestCase):
    @patch('file_access.download_file_from_s3')
    @patch('os.remove')
    @patch('builtins.open', new_callable=mock_open,
           read_data='{"Services": "test_value"}')
    def test_provide_file_path_file_opened(self, mock_file, _, __):
        s3_bucket = 'udacity-dend-capstone-project-weizhi-luo'
        s3_key = 'rail-service-performance/' \
                 '2022-06-09_Brighton_to_London Blackfriars.json'
        file_path = '/tmp/2022-06-09_Brighton_to_London Blackfriars.json'

        get_services_from_file_on_s3(s3_bucket, s3_key)

        mock_file.assert_called_with(file_path)
