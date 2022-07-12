from unittest import TestCase
from os.path import join

from file_tool import create_s3_key_for_file, get_file_name_without_extension,\
    change_file_extension, create_local_file_path_from_s3_path


class TestCreateS3KeyForFile(TestCase):
    def test_provide_file_name_s3_folder_return_s3_key_expected(self):
        file_name = 'test.json'
        s3_folder = 'test_s3_folder'
        expected_s3_key = join(s3_folder, file_name)

        s3_key = create_s3_key_for_file(file_name, s3_folder)

        self.assertEqual(s3_key, expected_s3_key)

    def test_provide_file_name_s3_folder_and_extra_folders_return_s3_expected(
            self):
        file_name = 'test.json'
        s3_folder = 'test_s3_folder'
        time = '00'
        date = '2022-06-01'
        expected_s3_key = join(s3_folder, time, date, file_name)

        s3_key = create_s3_key_for_file(file_name, s3_folder, time, date)

        self.assertEqual(s3_key, expected_s3_key)


class TestGetFileNameWithoutExtension(TestCase):
    def test_get_file_name_without_extension(self):
        file_path = r'ecmwf-weather-actual/netcdf/2022-06-21.nc'
        expected_file_name_without_extension = '2022-06-21'

        file_name_without_extension = \
            get_file_name_without_extension(file_path)

        self.assertEqual(file_name_without_extension,
                         expected_file_name_without_extension)


class TestChangeFileExtension(TestCase):
    def test_provide_file_name_with_extension_return_file_with_new_extension(
            self):
        file_name_with_extension = 'test.nc'
        new_extension = 'json'
        expected_file_name_with_changed_extension = 'test.json'

        file_name_with_changed_extension = \
            change_file_extension(file_name_with_extension, new_extension)

        self.assertEqual(file_name_with_changed_extension,
                         expected_file_name_with_changed_extension)

    def test_provide_file_name_without_extension_return_file_with_new_extension(
            self):
        file_name_without_extension = 'test2'
        new_extension = 'nc'
        expected_file_name_with_changed_extension = 'test2.nc'

        file_name_with_changed_extension = \
            change_file_extension(file_name_without_extension, new_extension)

        self.assertEqual(file_name_with_changed_extension,
                         expected_file_name_with_changed_extension)


class TestCreateLocalFilePathFromS3Path(TestCase):
    def test_return_expected_local_path(self):
        file_s3_path = 'udacity-dend-capstone-project-weizhi-luo/' \
                       'ecmwf-weather-actual/netcdf/2022-06-21.nc'
        expected_file_path = '/tmp/2022-06-21.nc'

        file_path = create_local_file_path_from_s3_path(file_s3_path)

        self.assertEqual(file_path, expected_file_path)
