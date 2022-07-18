## dms.station

Column Name | Data Type | Column Size for Display | Description                                                                            | Example 
--- |---------| --- |----------------------------------------------------------------------------------------|--- 
id | integer | | Unique identity of each station                                                        | 1
name | varchar | 50 | Name of the station                                                                    | London Bridge
code | varchar | 10 | Code of the station. This value is displayed in the railway historic performance data. | LBG

The column ```id``` is defined as the primary key ```station_pkey ``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

## dms.train_service_operator

Column Name | Data Type | Column Size for Display | Description                                                                                          | Example 
--- |---------| --- |------------------------------------------------------------------------------------------------------|--- 
id | integer | | Unique identity of each train service operator                                                       | 1
name | varchar | 50 | Name of the train service operator                                                                   | 	First MTR South Western Trains
code | varchar | 10 | Code of the train service opeator. This value is displayed in the railway historic performance data. | SW

The column ```id``` is defined as the primary key ```train_service_operator_pkey``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

## dms."date"

Column Name | Data Type | Description | Example 
--- |---|---| ---
"date" | date | Value of the date | 2022-06-01
"year" | integer | Year of the date | 2022
"month" | integer | Month of the date | 6
"day" | integer | Day of the date | 1
week | integer | Week number of the date | 22
day_of_week | integer |  Day of week of the date | 3

The column ```"date"``` is defined as the primary key ```date_pkey``` for this table. Its distribution style is 'ALL' so that it will be available in all compute node in Redshift to improve the performance of joins.

## dms.date_time

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

## dms.ecmwf_actual_coordinate

Column Name | Data Type | Column Size for Display | Description | Example 
--- |-----------|---|----------------------------|--- 
latitude | decimal | precision of 5 and scale of 2 | Latitude of the coordinate | 52.25
longitude | decimal | precision of 5 and scale of 2 | Longitude of the coordinate | -1.0

A composite primary key ```ecmwf_actual_coordinate_pkey``` is defined with columns ```latitude``` and ```longitude```.

## dms.ecmwf_forecast_coordinate

Column Name | Data Type | Column Size for Display | Description | Example 
--- |-----------|---|----------------------------|--- 
latitude | decimal | precision of 5 and scale of 2 | Latitude of the coordinate | 52.4
longitude | decimal | precision of 5 and scale of 2 | Longitude of the coordinate | -1.2

A composite primary key ```ecmwf_forecast_coordinate_pkey``` is defined with columns ```latitude``` and ```longitude```.

## fct.rail_service_performance

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

## fct.ecmwf_actual

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

## fct.ecmwf_forecast

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