# AWS Lamda functions

This is the repository for AWS lambda functions:
* download_ecmwf_forecast: function for downloading ECMWF forecast data
* convert_ecmwf_forecast_to_json: function for converting ECMWF forecast data from grib2 to json format which can be imported to Redshift data warehouse
* convert_ecmwf_actual_to_json: function for converting ECMWF actual data from netCDF to json format which can be import to Redshift data warehouse
* flatten_service_performance: function for extracting and flatten railway historic performance data to json format that can be import to Redshift data warehouse