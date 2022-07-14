# Udacity Data Engineering Capstone Project
Repository for Udacity Data Engineering capstone project.

## Introduction
The capstone project of Udacity's [Data Engineering](https://www.udacity.com/course/data-engineer-nanodegree--nd027) requires students to combine knowledge learned in the program to build a front to end solution covering the essential elements in data engineering.

This project gathers three data sets:
* Britain's national rail [historic service performance (HSP)](https://wiki.openraildata.com/index.php/HSP)
* European Centre for Medium-Range Weather Forecasts (ECMWF) [ERA5 hourly data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=overview)
* ECMWF [open data](https://www.ecmwf.int/en/forecasts/datasets/open-data)

The data extract, transform and load (ETL) pipeline is implemented using Apache [Airflow](https://airflow.apache.org/). Data is stored on an instance of [Amazon Redshift](https://aws.amazon.com/redshift/).

## Data Sets
Railway is important public transport system in Britain. Delays or cancellations cause inconvenience to a lot of railway users, especially the commuters. [Darwin](https://www.nationalrail.co.uk/100296.aspx) is rail industry's official training running information engine which provides not only real-time data, but also history service performance.

The disruptions can be caused by a lot of factors, such as over running engineering work, industrial actions, public holidays, weather, etc. Weather can bring impacts to railway in different ways. Severe weather conditions may damage railway signalling system or cause landslides and fallen trees on the tracks. Even a wrong kind of sunlight can cause delays as reported in this [news story](https://www.theguardian.com/uk-news/2016/jan/12/wrong-kind-of-sunlight-delays-southeastern-trains-london) from Guardian!

This project gathers three data sets including rail historic service performance and weather actual and forecast data. With the performance and weather actual data, data users may discover how weather conditions can affect train service performance. If such discovery achieved, the weather forecast data can be used for prediction. Or we may need more data for finding out the links between more factors and performance. This is the beginning of a journey :smile:!

### Rail Service Performance
Rail service performance data set is collected by registering an account at [Darwin](https://www.nationalrail.co.uk/100296.aspx). This [wiki](https://wiki.openraildata.com/index.php/HSP) page provides details about API access and data format. The data is in json format as:
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

Attribute ```date``` is extracted from the first 8 digits of rid. For example, 2016-07-01 is extracted from rid 201607013361763. Attribute ```delay_tolerance_minute``` is from the value of ```tolerance_value```. 

Attribute ```delayed``` is based on the values of ```num_not_tolerance``` and ```num_tolerance```. If the value of ```num_not_tolerance``` is 0 and the value of ```num_tolerance``` is 1, then ```delayed``` is false. Otherwise, it is true.

### ECMWF ERA5 Hourly Data
[ECMWF ERA5 hourly data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=overview) is ECMWF's fifth generation of reanalysis for global climate and weather for past decades. It combines both model data and observations worldwide to a global data set. This data set is used as actual weather data in this project. 

The following variables are downloaded:
* t2m: temperature 2 meters above Earth surface
* u10: eastward component of wind at the height of 10 meters above Earth surface
* v10: northward component of wind at the height of 10 meters above Earth surface
* msl: air pressure adjusted to the height of mean sea level
* sp: air pressure at the surface of land
* tcwv: Total amount of water vapour from Earth surface to the top of the atmosphere
* tp: Accumulated liquid and frozen water from rain and snow which falls to the Earth surface
* skt: Temperature of Earth surface

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


## ETL Pipeline



## Data Storage



## Discussion
