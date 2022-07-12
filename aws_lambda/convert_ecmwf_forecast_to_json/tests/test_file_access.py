from unittest import TestCase
from unittest.mock import patch

from file_access import get_xarray_dataset_from_file_on_s3, \
                               upload_data_to_s3


class TestGetXarrayDatasetFromFileOnS3(TestCase):
    @patch('file_access.download_file_from_s3')
    @patch('os.remove')
    @patch('xarray.open_dataset')
    def test_provide_file_path_without_engine_file_opened(
            self, mock_xarray_open_dataset, _, __):
        s3_bucket = 'udacity-dend-capstone-project-weizhi-luo'
        s3_key = 'ecmwf-weather-forecast/grib2/00/2022-06-22/' \
                 '2022-06-22_00_2t.grib2'
        file_path = '/tmp/2022-06-22_00_2t.grib2'

        get_xarray_dataset_from_file_on_s3(s3_bucket, s3_key, None)

        self.assertTrue(
            mock_xarray_open_dataset.call_args[0][0] == file_path and
            mock_xarray_open_dataset.call_args[1] == {})

    @patch('file_access.download_file_from_s3')
    @patch('os.remove')
    @patch('xarray.open_dataset')
    def test_provide_file_path_with_engine_specified_file_opened_with_engine(
            self, mock_xarray_open_dataset, _, __):
        s3_bucket = 'udacity-dend-capstone-project-weizhi-luo'
        s3_key = 'ecmwf-weather-forecast/grib2/00/2022-06-22/' \
                 '2022-06-22_00_2t.grib2'
        file_path = '/tmp/2022-06-22_00_2t.grib2'
        engine = 'cfgrib'

        get_xarray_dataset_from_file_on_s3(s3_bucket, s3_key, engine)

        self.assertTrue(
            mock_xarray_open_dataset.call_args[0][0] == file_path and
            mock_xarray_open_dataset.call_args[1]['engine'] == engine)


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

