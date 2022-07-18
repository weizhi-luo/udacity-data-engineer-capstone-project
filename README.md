# Udacity Data Engineering Capstone Project
Repository for Udacity Data Engineering capstone project.

## Introduction
The capstone project of Udacity's [Data Engineering](https://www.udacity.com/course/data-engineer-nanodegree--nd027) requires students to combine knowledge learned in the program to build a front to end solution covering the essential elements in data engineering.

This project gathers three data sets:
* Britain's national rail [historic service performance (HSP)](https://wiki.openraildata.com/index.php/HSP)
* European Centre for Medium-Range Weather Forecasts (ECMWF) [ERA5 hourly data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=overview)
* ECMWF [open data](https://www.ecmwf.int/en/forecasts/datasets/open-data)

The data extract, transform and load (ETL) pipeline is implemented using [Apache Airflow](https://airflow.apache.org/). Data is stored on an instance of [Amazon Redshift](https://aws.amazon.com/redshift/).

## Tools and Technologies
This project uses the following tools and technologies:
* Apache Airflow
* AWS Lambda
* AWS ECR
* Amazon Redshift
* Amazon S3

### Apache Airflow
[Apache Airflow](https://airflow.apache.org/) is an open-source platform for managing data engineering pipelines. Airflow uses a DAG (Directed Acyclic Graph) to represent a collection of tasks:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/ecmwf_actual_dag.png">

Airflow's user interface makes it easy to manage DAG runs. It helps users check status and log of DAG runs, trigger DAG runs, and manage variables and connections used by DAGs. Apart from user interface, Airflow also provides a command line interface for various operations, such as DAG backfill, etc.

Airflow's scheduler allows users to set up different schedules that trigger DAGs to run. Users define a DAG by a Python script, which represents a collection of tasks. Each task is defined by instantiating an [operator](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html), where users can use Jinja Templating to access [macros](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#templates-ref) and [variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) without hard coding the tasks. 

Airflow also provides [connections and hooks](https://airflow.apache.org/docs/apache-airflow/stable/concepts/connections.html) that make it simpler to define tasks for pulling and pushing data from and to various sources (http, ftp, sftp, Amazon S3, etc.). Except the inbuilt and third party contribute operators, Airflow also allows users to define custom operators for specific needs.

Airflow is a convenient and powerful tool for implementing a data engineering pipeline. Although technically users can place all data extraction and processing logics in it, it is a better practice to use it as an [orchestrator](https://www.astronomer.io/guides/dag-best-practices/) that invokes heavy processing jobs on other platforms or instances. Therefore, AWS solutions are also used in this project.

### AWS Lambda and Amazon ECR
[AWS Lambda](https://aws.amazon.com/lambda/) is a serverless and event-driven compute service which allows users to run codes for different applications without setting up a server.

As discussed above, it is a good practise to use Airflow as an orchestrator for triggering heavy processing jobs elsewhere. Therefore, in this project AWS Lambda is used for various tasks, such as data conversion, processing and validation. 

AWS Lambda supports different ways of deployments. The simplest way is to write codes in their web code editor. For more complex applications, users can upload a compressed file (.zip file archive) directly. User can also upload the zip file to S3 and point AWS Lambda to the S3 path. 

But there are limits for zipped and unzipped package deployment: 50 MB for zipped and 250 MB for unzipped. If the package exceeds these limits, users have to build code in a [container image](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html), upload the image to [Amazon ECR](https://aws.amazon.com/ecr/) and direct AWS Lambda to use the image. The size limit of a container image is 10 GB.

### Amazon Redshift
[Amazon Redshift](https://aws.amazon.com/redshift/) is cloud data warehouse which uses SQL to analyze structured and unstructured data from operational databases, data lakes, etc. Amazon Redshift is based on [PostgreSQL](https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-and-postgres-sql.html) and they share similar syntax and data types definition. However, they are different in several ways. 

Amazon Redshift stores data in columns. Tables are organized in columns instead of rows, which provides better I/O characteristics for analytical workloads.

Amazon Redshift does not [enforce constraints](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html). Primary and foreign keys can be defined but they are not enforced. They are informational and query optimizer uses them to generate more efficient execution plans.

Amazon Redshift does not support indexes. Users need to understand data structure and usage requirements to define [sort keys](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html), [distribution keys and styles](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html) for query performance improvement.

Amazon Redshift is distributed and comprised of several compute nodes which are orchestrated by one leader node. Users can vertically scale up Redshift by adding more processing power and memory to compute nodes or horizontally scale out by adding more compute nodes. 

### Amazon S3
[Amazon S3](https://aws.amazon.com/s3/) is an object storage service. It is scalable and cost-effective. It is integrated with Amazon Redshift that allows easy data import. In this project, Amazon S3 is used as the data lake which stored raw data extracted from various sources and processed data for Redshift import.


## Data Sets
Railway is important public transport system in Britain. Delays or cancellations cause inconvenience to a lot of railway users, especially the commuters. The disruptions can be caused by a lot of factors, such as over running engineering work, industrial actions, public holidays, weather, etc. Weather can bring impacts to railway in different ways. Severe weather conditions may damage railway signalling system or cause landslides and fallen trees on the tracks. Even a wrong kind of sunlight can cause delays as reported in this [news story](https://www.theguardian.com/uk-news/2016/jan/12/wrong-kind-of-sunlight-delays-southeastern-trains-london) from Guardian!

This project gathers three data sets, including rail historic service performance, actual and forecast weather data. With the performance and actual weather data, data users may discover how weather conditions can affect train service performance. If such discovery achieved, the weather forecast data can be used for prediction. Or we may need more data for finding out the links between more factors and performance. This is the beginning of a journey :smile:!

### Rail Service Performance
Rail service performance data set is available at [Darwin](https://www.nationalrail.co.uk/100296.aspx). Darwin is Britain's rail industry's official training running information engine which provides both real-time and history service performance data. A user account has to be registered with Darwin API. And this [wiki](https://wiki.openraildata.com/index.php/HSP) page provides details about API access and data format. The data is in json format as:
```json
{
  "header": {
    "from_location": "BTN",
    "to_location": "VIC"
  },
  "Services": [
    {
      "serviceAttributesMetrics": {
        "origin_location": "BTN",
        "destination_location": "VIC",
        "gbtt_ptd": "0712",
        "gbtt_pta": "0823",
        "toc_code": "GX",
        "matched_services": "1",
        "rids": [ "201607013361753" ]
      },
      "Metrics": [
        {
          "tolerance_value": "0",
          "num_not_tolerance": "0",
          "num_tolerance": "1",
          "percent_tolerance": "100",
          "global_tolerance": true
        },
        {
          "tolerance_value": "5",
          "num_not_tolerance": "0",
          "num_tolerance": "1",
          "percent_tolerance": "100",
          "global_tolerance": false
        }
      ]
    },
    {
      "serviceAttributesMetrics": {
        "origin_location": "BTN",
        "destination_location": "VIC",
        "gbtt_ptd": "0729",
        "gbtt_pta": "0839",
        "toc_code": "GX",
        "matched_services": "1",
        "rids": [ "201607013361763" ]
      },
      "Metrics": [
        {
          "tolerance_value": "0",
          "num_not_tolerance": "1",
          "num_tolerance": "0",
          "percent_tolerance": "0",
          "global_tolerance": true
        },
        {
          "tolerance_value": "5",
          "num_not_tolerance": "0",
          "num_tolerance": "1",
          "percent_tolerance": "100",
          "global_tolerance": false
        }
      ]
    },
    ...
  ]
}
```
What we are interested here are the attributes of:
* origin_location: station code where the train is from
* destination_location: station code where the train is to
* gbtt_ptd: departure time
* gbtt_pta: arrival time
* toc_code: train service operator's code
* rids: id of the service
* tolerance_value: delay tolerance value in minute
* num_not_tolerance: number of services that are delayed above the tolerance value
* num_tolerance: number of services that are delayed within the tolerance value
* percent_tolerance: percentage of services that are delayed within the tolerance value

As the attributes of tolerance_value, num_not_tolerance, num_tolerance and percent_tolerance are in one of the Metrics, the ETL pipeline has to extract Services data and flatten and convert them to a format as below for data upload to Redshift data warehouse:
```json
{  "origin_location": "BTN",  "destination_location": "VIC",  "date": "2016-07-01",  "departure_time": "0712",  "arrival_time": "0823",  "operator_code": "GX",  "rid": "201607013361753",  "delay_tolerance_minute": "0",  "delayed": false}
{  "origin_location": "BTN",  "destination_location": "VIC",  "date": "2016-07-01",  "departure_time": "0712",  "arrival_time": "0823",  "operator_code": "GX",  "rid": "201607013361753",  "delay_tolerance_minute": "5",  "delayed": false}
{  "origin_location": "BTN",  "destination_location": "VIC",  "date": "2016-07-01",  "departure_time": "0729",  "arrival_time": "0839",  "operator_code": "GX",  "rids": "201607013361763",  "delay_tolerance_minute": "0",  "delayed": true}
{  "origin_location": "BTN",  "destination_location": "VIC",  "date": "2016-07-01",  "departure_time": "0729",  "arrival_time": "0839",  "operator_code": "GX",  "rids": "201607013361763",  "delay_tolerance_minute": "5",  "delayed": false}
```
Three new attributes are generated in this conversion and flatten process:
* date
* delay_tolerance_minute
* delayed

Attribute ```date``` is extracted from the first 8 digits of rid. For example, 2016-07-01 is extracted from rid **20160701**3361763. Attribute ```delay_tolerance_minute``` is from the value of ```tolerance_value```. 

Attribute ```delayed``` is based on the values of ```num_not_tolerance``` and ```num_tolerance```. If the value of ```num_not_tolerance``` is 0 and the value of ```num_tolerance``` is 1, then ```delayed``` is false. Otherwise, it is true.

### ECMWF ERA5 Hourly Data
[ECMWF ERA5 hourly data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=overview) is ECMWF's fifth generation of reanalysis for global climate and weather for past decades. It combines both model data and observations worldwide to a global data set. This data set is used as actual weather data in this project. 

The following variables are downloaded:
* t2m: temperature 2 meters above Earth surface
* u10: eastward component of wind at the height of 10 meters above Earth surface
* v10: northward component of wind at the height of 10 meters above Earth surface
* msl: air pressure adjusted to the height of mean sea level
* sp: air pressure at the surface of land
* tcwv: total amount of water vapour from Earth surface to the top of the atmosphere
* tp: accumulated liquid and frozen water from rain and snow which falls to the Earth surface
* skt: temperature of Earth surface

As we are checking rail service performance data around London region, ERA5 data is downloaded within the region of north latitude 52.5&deg;, south latitude 50.5&deg;, east longitude 1&deg; and west longitude -1&deg;.

The data is downloaded in [netCDF](https://en.wikipedia.org/wiki/NetCDF) format and the ETL pipeline needs to convert it to json format as below for data import to Redshift:
```json
{"latitude": 52.5, "longitude": -1.0, "value_date_time": "2022-06-10T00:00:00.000000000", "t2m": 288.0147399902344, "u10": 3.1395483016967773, "v10": 4.164521217346191, "msl": 101545.5, "sp": 100141.15625, "tcwv": 14.827731132507324, "tp": 3.335881046950817e-06, "skt": 286.8713073730469}
{"latitude": 52.5, "longitude": -1.0, "value_date_time": "2022-06-10T01:00:00.000000000", "t2m": 287.41796875, "u10": 3.6724212169647217, "v10": 3.777169942855835, "msl": 101558.3125, "sp": 100151.34375, "tcwv": 13.847014427185059, "tp": 1.4290708350017667e-06, "skt": 286.52166748046875}
```

### ECMWF Open Data
[ECMWF open data](https://www.ecmwf.int/en/forecasts/datasets/open-data) is a publicly available weather forecast data set published from [European Centre for Medium-Range Weather Forecasts](https://www.ecmwf.int/) medium-range and seasonal forecast models. This data set is used as forecast weather data in this project. ECMWF publishes data four times a day: 00 hour, 06 hour, 12 hour and 18 hour. Two publishes are used: 00 hour and 12 hour. 

The following variables are downloaded:
* t2m: temperature 2 meters above Earth surface
* u10: eastward component of wind at the height of 10 meters above Earth surface
* v10: northward component of wind at the height of 10 meters above Earth surface
* msl: air pressure adjusted to the height of mean sea level
* sp: air pressure at the surface of land
* tcwv: Total amount of water vapour from Earth surface to the top of the atmosphere
* tp: Accumulated liquid and frozen water from rain and snow which falls to the Earth surface
* skt: Temperature of Earth surface

The data is downloaded using a [package](https://github.com/ecmwf/ecmwf-opendata) for simplicity. However, it appears that the variables in ECMWF open data is generated at different altitude level. If all variables are downloaded in the same time, there can be problems. Therefore, the ETL pipeline will download data variable by variable. 

As we are checking rail service performance data around London region, data is downloaded within the region of north latitude 52.5&deg;, south latitude 50.5&deg;, east longitude 1&deg; and west longitude -1&deg;.

The data is downloaded in [grib2](https://rda.ucar.edu/datasets/ds083.2/software/README_Formats.pdf) format and the ETL pipeline has to convert it to the following json format for data import to Redshift. Let's use eastward component of wind at the height of 10 meters above Earth surface (u10) as an example:
```json
{"latitude": 52.4, "longitude": -1.2, "forecast_date_time": "2022-06-21T00:00:00.000000000", "value_date_time": "2022-06-21T00:00:00.000000000", "u10": 1.878143310546875}
{"latitude": 52.4, "longitude": -1.2, "forecast_date_time": "2022-06-21T00:00:00.000000000", "value_date_time": "2022-06-21T03:00:00.000000000", "u10": -0.2327423095703125}
```

## Data Model
The data warehouse is using a dimension data model based on [star schema](https://en.wikipedia.org/wiki/Star_schema). Comparing to normalized data models, dimension data model simplifies joins between tables and also facilitate aggregation. There are three fact tables storing ECMWF actual data, ECMWF forecast data and rail service performances. Dimension tables contains data for railway stations, train service operator, ECMWF actual data coordinates, ECMWF forecast data coordinates, date time and date. 

The data dictionary is available at [file](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/aws_redshift/DATA_DICTIONARY.md). The SQL scripts for creating database schema and table are available in [repository](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/tree/main/aws_redshift):
* 001_create schemas.sql 
* 002_create_staging_tables.sql 
* 003_create_dimension_tables.sql 
* 004_create_fact_tables.sql

### Database Schema
Three schema are created for different data:
* stg: schema for staging data
* dms: schema for dimension data
* fct: schema for fact data

### Staging Tables
Eighteen staging tables are created to allow transformed raw data to be imported to the data warehouse:
* stg.rail_service_performance: staging table for rail service performance data
* stg.ecmwf_actual: staging table for ECMWF actual data
* stg.ecmwf_forecast_00_2t: staging table for ECMWF temperature data published at 00 hour
* stg.ecmwf_forecast_00_msl: staging table for ECMWF mean sea level pressure data published at 00 hour
* stg.ecmwf_forecast_00_10v: staging table for ECMWF northward component of wind published at 00 hour
* stg.ecmwf_forecast_00_10u: staging table for ECMWF eastward component of wind published at 00 hour
* stg.ecmwf_forecast_00_tp: staging table for accumulated liquid and frozen water data published at 00 hour
* stg.ecmwf_forecast_00_tcwv: staging table for total amount of water vapour data published at 00 hour 
* stg.ecmwf_forecast_00_sp: staging table for air pressure at the surface of land published at 00 hour
* stg.ecmwf_forecast_00_skt: staging table for earth surface temperature data published at 00 hour
* stg.ecmwf_forecast_12_2t: staging table for ECMWF temperature data published at 12 hour
* stg.ecmwf_forecast_12_msl: staging table for ECMWF mean sea level pressure data published at 12 hour
* stg.ecmwf_forecast_12_10v: staging table for ECMWF northward component of wind published at 12 hour
* stg.ecmwf_forecast_12_10u: staging table for ECMWF eastward component of wind published at 12 hour
* stg.ecmwf_forecast_12_tp: staging table for accumulated liquid and frozen water data published at 12 hour
* stg.ecmwf_forecast_12_tcwv: staging table for total amount of water vapour data published at 12 hour 
* stg.ecmwf_forecast_12_sp: staging table for air pressure at the surface of land published at 12 hour
* stg.ecmwf_forecast_12_skt: staging table for earth surface temperature data published at 12 hour

### Dimension Tables
Six dimension tables are created to store data describing attributes of fact data:
* dms.station
* dms.train_service_operator
* dms."date"
* dms.date_time
* dms.ecmwf_actual_coordinate
* dms.ecmwf_forecast_coordinate

#### dms.station
dms.station is a dimension table for railway station name and code. The data is obtained from [National Rail](https://www.nationalrail.co.uk/stations_destinations/48541.aspx).

The table has the following columns:

Column Name | Data Type | Column Size for Display | Description                                                                            | Example 
--- |---------| --- |----------------------------------------------------------------------------------------|--- 
id | integer | | Unique identity of each station                                                        | 1
name | varchar | 50 | Name of the station                                                                    | London Bridge
code | varchar | 10 | Code of the station. This value is displayed in the railway historic performance data. | LBG

The column ```id``` is defined as the primary key ```station_pkey ``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

#### dms.train_service_operator
dms.train_service_operator is a dimension table for train service operator name and code. The data is extracted from the operator_code column of stg.rail_service_performance and combined with data from railway codes [website](http://www.railwaycodes.org.uk/operators/toccodes.shtm).

The table has the following columns:

Column Name | Data Type | Column Size for Display | Description                                                                                          | Example 
--- |---------| --- |------------------------------------------------------------------------------------------------------|--- 
id | integer | | Unique identity of each train service operator                                                       | 1
name | varchar | 50 | Name of the train service operator                                                                   | 	First MTR South Western Trains
code | varchar | 10 | Code of the train service opeator. This value is displayed in the railway historic performance data. | SW

The column ```id``` is defined as the primary key ```train_service_operator_pkey``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

#### dms."date"
dms."date" is a dimension table for date data extracted from date column from staging table stg.rail_service_performance.

The table has the following columns:

Column Name | Data Type | Description | Example 
--- |---|---| ---
"date" | date | Value of the date | 2022-06-01
"year" | integer | Year of the date | 2022
"month" | integer | Month of the date | 6
"day" | integer | Day of the date | 1
week | integer | Week number of the date | 22
day_of_week | integer |  Day of week of the date | 3

The column ```"date"``` is defined as the primary key ```date_pkey``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

#### dms.date_time
dms.date_time is a dimension table for date and time extracted from value_date_time column from staging tables of ECMWF actual and forecast data.

The table has the following columns:

Column Name | Data Type | Description | Example 
--- | --- |---| ---
date_time | timestamp | Value of the date and time | 2022-06-01 00:00:00
"year" | integer | Year of the date and time | 2022
"month" | integer | Month of the date and time | 6
"day" | integer | Day of the date and time | 1
"hour" | integer | Hour of the date and time | 0
week | integer | Week number of the date | 22
day_of_week | integer | Day of week of the date | 3

The column ```date_time``` is defined as the primary key ```date_time_pkey``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

#### dms.ecmwf_actual_coordinate
dms.ecmwf_actual_coordinate is a dimension table for coordinate data extracted from staging table of ECMWF actual data.

The table has the following columns:

Column Name | Data Type | Column Size for Display | Description | Example 
--- |-----------|---|----------------------------|--- 
latitude | decimal | precision of 5 and scale of 2 | Latitude of the coordinate | 52.25
longitude | decimal | precision of 5 and scale of 2 | Longitude of the coordinate | -1.0

A composite primary key ```ecmwf_actual_coordinate_pkey``` is defined with columns ```latitude``` and ```longitude```.

#### dms.ecmwf_forecast_coordinate
dms.ecmwf_forecast_coordinate is a dimension table for coordinate data extracted from staging tables of ECMWF forecast data.

Column Name | Data Type | Column Size for Display | Description | Example 
--- |-----------|---|----------------------------|--- 
latitude | decimal | precision of 5 and scale of 2 | Latitude of the coordinate | 52.4
longitude | decimal | precision of 5 and scale of 2 | Longitude of the coordinate | -1.2

A composite primary key ```ecmwf_forecast_coordinate_pkey``` is defined with columns ```latitude``` and ```longitude```.

#### Why two coordinate tables?
The creation of two dimension tables for coordinates of ECMWF actual and forecast is due to the difference in granularity. ECMWF actual data's coordinates form a latitude-longitude grid of 0.25 degrees, while ECMWF forecast data's grid is of 0.4 degrees. By having two dimension tables, mapping tables can be created later to fill the gap between the difference in granularity.

### Fact Tables
Three fact tables are created to store fact data:
* fct.rail_service_performance
* fct.ecmwf_actual
* fct.ecmwf_forecast

#### fct.rail_service_performance
fct.rail_service_performance table contains rail service performance data. It has the following columns:

Column Name | Data Type | Column Size for Display | Description                                                                                                                                                                | Example 
--- | --- | --- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---
origin_location_id | integer | | Unique identity of origin station. Foreign key to dimension table dms.station.                                                                                             | 1908
destination_location_id | integer | | Unique identity of destination station. Foreign key to dimension table dms.station.                                                                                        | 1447
"date" | date | | Value of the date of the service. foreign key to dimension table dms."date".                                                                                               | 2022-06-20
arrival_time | time | | Arrival time of the service                                                                                                                                                | 06:50
departure_time | time | | Departure time of the service                                                                                                                                              | 06:11
train_service_operator_id | integer | | Unique identity of each train service operator. Foreign key to dimension table dms.train_service_operator                                                                  | 1
rid | varchar | 30 | Identifier of the service                                                                                                                                                  | 202207078781536
delayed | boolean | | Indicate whether the service is delayed                                                                                                                                    | true
delay_tolerance_minute | smallint | | Delay tolerance in minutes. For example, 5 means the delay is tolerated up to 5 minutes. Any delay under 5 minutes will have value of ```false``` in column ```delayed```. | 5

fct.rail_service_performance is linked to dms.station, dms."date" and dms.train_service_operator as:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.rail_links.png"/>

Here is an example for querying the related tables. Assuming that a user wants to find out the delay percentages of the destination stations of week 24 of 2022, the following query can be used:
```postgresql
SELECT all_services.name, 
CASE WHEN delayed_services.name IS NULL THEN 0
ELSE (CAST(delayed_services.service_count AS DECIMAL(5, 2)) / CAST(all_services.service_count AS DECIMAL(5, 2))) * 100
END AS delay_percentage
FROM (
  SELECT ds.name, COUNT(1) AS service_count
  FROM fct.rail_service_performance p
  INNER JOIN dms.date d
  ON p."date" = d."date"
  INNER JOIN dms.station ds
  ON ds.id = p.destination_location_id
  WHERE d."week" = 24
  GROUP BY ds.name
) AS all_services
LEFT JOIN (
  SELECT ds.name, COUNT(1) AS service_count
  FROM fct.rail_service_performance p
  INNER JOIN dms.date d
  ON p."date" = d."date"
  INNER JOIN dms.station ds
  ON ds.id = p.destination_location_id
  WHERE d."week" = 24 AND p.delayed = true
  GROUP BY ds.name
) AS delayed_services
ON delayed_services.name = all_services.name
ORDER BY delay_percentage 
```
The result of the query is:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/query_result.PNG">

#### fct.ecmwf_actual
fct.ecmwf_actual table contains ECMWF actual data. It has the following columns:

Column Name | Data Type        | Column Size for Display | Description                                                                              | Example
---|------------------|---|------------------------------------------------------------------------------------------| ---
latitude | decimal          | precision of 5 and scale of 2 | Latitude of the coordinate. Foregin key to dimension table dms.ecmwf_actual_coordinate.  | 52.25
longitude | decimal          | precision of 5 and scale of 2 | Longitude of the coordinate. Foregin key to dimension table dms.ecmwf_actual_coordinate. | -1.0
value_date_time | timestamp        | | Value date and time. Foreign key to dimension table dms.date_time.                       | 2022-06-10 00:00:00
temperature | double precision | | Temperature                                                                              | 288.0147399902344
u_wind_component | double precision | | Eastward component of wind at the height of 10 meters above Earth surface                | 3.1395483016967773
v_wind_component | double precision | | Northward component of wind at the height of 10 meters above Earth surface               | 4.164521217346191
mean_sea_level_pressure | double precision | | Air pressure adjusted to the height of mean sea level                                    | 101545.5
surface_pressure | double precision | | Air pressure at the surface of land                                                      | 100141.15625
total_column_vertically_integrated_water_vapour | double precision | | Total amount of water vapour from Earth surface to the top of the atmosphere |  14.827731132507324
total_precipitation | double precision | | accumulated liquid and frozen water from rain and snow which falls to the Earth surface | 0
skin_temperature | double precision | | temperature of Earth surface | 286.8713073730469

fct.ecmwf_actual is linked to dms.ecmwf_actual_coordinate and dms.date_time as:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.ecmwf_actual_links.png"/>

#### fct.ecmwf_forecast
fct.ecmwf_forecast table contains ECMWF forecast data. It has the following columns:

Column Name | Data Type        | Column Size for Display | Description                                                                                | Example
---|------------------|---|--------------------------------------------------------------------------------------------| ---
latitude | decimal          | precision of 5 and scale of 2 | Latitude of the coordinate. Foregin key to dimension table dms.ecmwf_forecast_coordinate.  | 52.4
longitude | decimal          | precision of 5 and scale of 2 | Longitude of the coordinate. Foregin key to dimension table dms.ecmwf_forecast_coordinate. | -1.2
forecast_date_time | timestamp | | Date and time when the forecast is published. | 2022-06-09 12:00:00:00 
value_date_time | timestamp        | | Value date and time. Foreign key to dimension table dms.date_time.                         | 2022-06-10 00:00:00
temperature | double precision | | Temperature                                                                                | 288.0147399902344
u_wind_component | double precision | | Eastward component of wind at the height of 10 meters above Earth surface                  | 3.1395483016967773
v_wind_component | double precision | | Northward component of wind at the height of 10 meters above Earth surface                 | 4.164521217346191
mean_sea_level_pressure | double precision | | Air pressure adjusted to the height of mean sea level                                      | 101545.5
surface_pressure | double precision | | Air pressure at the surface of land                                                        | 100141.15625
total_column_vertically_integrated_water_vapour | double precision | | Total amount of water vapour from Earth surface to the top of the atmosphere               |  14.827731132507324
total_precipitation | double precision | | accumulated liquid and frozen water from rain and snow which falls to the Earth surface    | 0
skin_temperature | double precision | | temperature of Earth surface                                                               | 286.8713073730469

Column ```forecast_date_time``` is defined as the distribution key so that the table is partitioned based on it.

fct.ecmwf_forecast is linked to dms.ecmwf_forecast_coordinate and dms.date_time as:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.ecmwf_forecast_links.png"/>

## ETL Pipeline
The ETL pipeline is implemented by Apache Airflow. There are nine DAGs set up:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/airflow_dags.png"/>

The DAGs can be categorised into two groups:
* DAGs run daily
* DAG run once

The source codes are available in [repository](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/tree/main/airflow/DAGs).

### Daily DAGs
The daily DAGs run once per day for extracting and transforming data sets from rail service performance, ECMWF actual and forecast data.

#### Rail Service Performance DAGs
Five DAGs are created to extract and transform rail service performance data. Each DAG is schedule with cron ```0 0 * * 1-5``` and downloads data of ONE day, which is the weekday before today:
* daily_rail_london_blackfriars_inbound_services_performance_download
* daily_rail_london_bridge_inbound_services_performance_download
* daily_rail_london_kings_cross_inbound_services_performance_download
* daily_rail_london_marylebone_inbound_services_performance_download
* daily_rail_london_waterloo_inbound_services_performance_download

Take DAG daily_rail_london_blackfriars_inbound_services_performance_download as an example:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/blackfriars_dag.png"/>

There are five tasks in this DAG. 

The first task download_Brighton_to_London_Blackfriars downloads data from National Rail's Darwin API. The download is done by using the http connection national_rail_historical_service_performance set up in Airflow. 

The second task data_quality_check_Brighton_to_London_Blackfriars receives the downloaded data via xcom and validates its format. 

The third task upload_Brighton_to_London_Blackfriars uploads validated data to an AWS bucket. The bucket name and S3 key to the file are sent to the fourth task flatten_Brighton_to_London_Blackfriars via xcom. 

The fourth task invokes the AWS Lamda function flatten_service_performance (source code is at [repository](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/tree/main/aws_lambda/flatten_service_performance)) to convert and flatten data to the format explained in Data Sets section above. 

Finally, a watcher task is used and only activate if any of the previous task fails.

#### ECMWF Forecast DAGs
Two DAGs are created to download ECMWF forecast data:
* daily_ecmwf_00_forecast_download
* daily_ecmwf_12_forecast_download

daily_ecmwf_00_forecast_download runs with schedule ```0 10 * * *``` and gets forecast data published at hour 00. The reason why it is scheduled to run just before 10am is that data published at hour 00 will only be completely available in 8 to 9 hours. Similarly, daily_ecmwf_12_forecast_download is with schedule ```0 22 * * *``` for downloading data published at hour 12.

Take DAG daily_ecmwf_00_forecast_download as an example:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/ecmwf_00_forecast_dag.png"/>

As explained before, in order to avoid problems when downloading all eight variables in call, each variable is downloaded independently. Take temperature of Earth surface, skt, as an example:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/ecmwf_00_forecast_skt_dag.png">

There are six tasks in downloading this data. 

The first task download_ecmwf_forecast_skt invokes an AWS Lambda function download_ecmwf_forecast to download data in grib2 format and upload to S3 bucket. The Lambda function returns information related to the downloaded file such as S3 bucket and key and pushes it to xcom.

The second task create_conversion_payload_ecmwf_forecast_skt pulls the information from xcom and prepares them as the payload to be used by the third task. The prepared payload is pushed to xcom.

The third task convert_ecmwf_forecast_to_json_skt pulls data from xcom and uses it as the payload for invoking AWS Lambda function convert_ecmwf_forecast_to_json. This Lambda function downloads the grib2 file and converts it to the json format that can be imported to Redshift data warehouse. After the conversion is done, it returns the S3 bucket and S3 key for the new file's location. The S3 bucket and S3 key are pushed to xcom.

The fourth task create_json_file_validation_payload_skt pulls S3 bucket and S3 key from xcom. From variables set in Airflow, it also gets the expected keys to be available in the converted file. With both part of information, it creates the payload to be used by the next task. The prepared payload is pushed to xcom.

The fifth task validate_json_file_skt pulls data from xcom and uses it as the payload for invoking AWS Lambda function redshift_json_file_format_check. The Lambda function downloads the json file and check if its format and content.

Finally, a watcher task is used and only activate if any of the previous task fails.

AWS Lambda functions are used here to offload heavy data processing from Airflow, as well as avoid installing too many packages and libraries to the server where Airflow is installed.

#### ECMWF Actual DAG
One DAG is created to download ECMWF actual data: daily_ecmwf_actual_download. As ECMWF ERA5 data is normally delayed for around 5 days. In order to make sure data is available, the dag downloads data of seven days ago.

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/ecmwf_actual_dag.png">

There are seven tasks in this DAG.

The first task prepare_download_path prepares the local path for storing ECMWF actual data file.

The second task download_file downloads ECMWF actual data in file of netCDF format following the instruction from the dataset [website](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form) and API access [website](https://cds.climate.copernicus.eu/api-how-to).

The third task upload_file uploads ECMWF actual data file to S3 bucket and push the related information to xcom. 

The fourth task convert_to_json pulls data from xcom, creates payload and invokes the AWS Lambda function convert_ecmwf_actual_json for converting netCDF data to a format of json that can be imported to Redshift. The S3 bucket and S3 key are returned and push to xcom.

The fifth task create_json_file_validation_payload pulls S3 bucket and S3 key from xcom. From variables set in Airflow, it also gets the expected keys to be available in the converted file. With both part of information, it creates the payload to be used by the next task. The prepared payload is pushed to xcom.

The sixth task validate_json_file pulls data from xcom and uses it as the payload for invoking AWS Lambda function redshift_json_file_format_check. The Lambda function downloads the json file and check if its format and content.

Finally, a watcher task is used and only activate if any of the previous task fails.

Similar to ECMWF forecast DAGs, an AWS Lambda function is used here to offload heavy data processing of netCDF/json conversion from Airflow. However, data download is still carried out by Airflow. The reason is that data API key has to be saved in .cdsapirc file under $HOME folder. I haven't found a way to do that using AWS Lambda. It might be achievable using a customised docker image. It is worth further investigation and tests.

### One Time DAG
The one time DAG one_time_data_import runs once with schedule ```@once```. It can be divided into three steps:
* staging: copy transformed raw data to staging tables on Redshift instance
* dimension: from staging tables, extract data to load dimension tables
* fact: by joining staging and dimension tables, load data to fact tables

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/one_time_dag.png">

#### Staging
Tasks copy_staging_ecmwf_actual and copy_staging_rail_service_performance connect to Redshift instance and invoke the ```copy``` command to load data from json files in S3 to staging tables.

Task groups copy_staging_ecmwf_forecast_00 and copy_staging_ecmwf_forecast_12 execute Redshift's ```copy``` command to load data from json files in S3 specified by manifests to staging tables.

#### Dimension
Tasks load_dimension_date, load_dimension_date_time, load_dimension_ecmwf_actual_coordinates, load_dimension_ecmwf_forecst_coordinates and load_dimension_train_service run the SQL scripts defined in LoadDimensionTables class in [sql_queries.py](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/airflow/plugins/helpers/sql_queries.py) file in Redshift instance to load dimension tables.

Task copy_dimension_station_codes executes ```copy``` command to load railway stations and codes data from csv file in S3 to dimension table.

#### Fact
Tasks load_fact_ecmwf_actual, load_fact_ecmwf_forecast and load_fact_rail_service_performance run the SQL scripts defined in LoadFactTables class in [sql_queries.py](https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/airflow/plugins/helpers/sql_queries.py) file in Redshift instance to load fact tables.

#### Import Results
After running the import DAG, staging, dimension and fact tables are filled with data. Some results are presented as follows.

Query result of table fct.ecmwf_actual:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.ecmwf_actual_result.PNG">

Query result of table fct.ecmwf_forecast:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.ecmwf_forecast_result.PNG">

Query result of table fct.rail_service_performance:

<img src="https://github.com/weizhi-luo/udacity-data-engineer-capstone-project/blob/main/doc/images/fct.rail_service_performance_result.PNG">

## Discussion

### If the data was increased by 100x
If the data was increased by 100 times, the current ETL pipeline may not be able to handle that. We may use large scale data processing engine such as Apache Spark instead.

### If the pipelines were run on a daily basis by 7am
We need to make sure the schedules in Airflow are set to by 7am, such as cron ```0 6 * * *```. The Airflow's scheduler runs job at the end of schedule_interval. 

For example, assuming that the start date is 2022-06-01 06:00 and schedule is ```0 6 * * *```. The run with start date 2022-06-01 06:00 will only be triggered soon after 2022-06-02 05:59.

### If the database needed to be accessed by 100+ people
There can be several actions to be done to help solve this challenge.
1. Horizontally scale up Redshift by adding more computing nodes so that queries can be executed in parallel.
2. Analyze business requirements from the users and different data marts might be created to satisfy different needs.
