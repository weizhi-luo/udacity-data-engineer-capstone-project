from unittest import TestCase

from file_tool import create_local_file_path_from_s3_path


class TestCreateLocalFilePathFromS3Path(TestCase):
    def test_create_local_file_path_from_s3_path(self):
        file_s3_path = 'udacity-dend-capstone-project-weizhi-luo/' \
                       'ecmwf-weather-actual/netcdf/2022-06-21.nc'
        expected_file_path = '/tmp/2022-06-21.nc'

        file_path = create_local_file_path_from_s3_path(file_s3_path)

        self.assertEqual(file_path, expected_file_path)
