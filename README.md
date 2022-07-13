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

The disruptions can be caused by a lot of factors, such as over running engineering work, industrial actions, public holidays, weather, etc. Weather can bring impacts to railway in different ways. Severe weather conditions may damage railway signalling system or cause landslides and fallen trees on the tracks. Even a wrong kind of sunlight can cause delays as reported in the [news story](https://www.theguardian.com/uk-news/2016/jan/12/wrong-kind-of-sunlight-delays-southeastern-trains-london) from Guardian!

This project gathers three data sets including rail historic service performance and weather actual and forecast data. With the performance and weather actual data, data users may discover how weather conditions can affect train service performance. If such discovery achieved, the weather forecast data can be used for prediction. Or we may need more data for finding out the links between more factors and performance. This is the beginning of a journey :smile:!


## ETL Pipeline



## Data Storage



## Discussion
