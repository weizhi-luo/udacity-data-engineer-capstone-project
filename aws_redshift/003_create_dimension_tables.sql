CREATE TABLE dms.station (
    id INTEGER IDENTITY(1, 1) NOT NULL,
    name VARCHAR(50) NOT NULL,
    code VARCHAR(10) NOT NULL,
    CONSTRAINT station_pkey PRIMARY KEY (id)
)
DISTSTYLE ALL;


CREATE TABLE dms.train_service_operator (
    id INTEGER IDENTITY(1, 1) NOT NULL,
    name VARCHAR(50) NOT NULL,
    code VARCHAR(10) NOT NULL,
    CONSTRAINT train_service_operator_pkey PRIMARY KEY (id)
)
DISTSTYLE ALL;


CREATE TABLE dms.ecmwf_actual_coordinate (
    latitude DECIMAL(5, 2) NOT NULL,
    longitude DECIMAL(5, 2) NOT NULL,
    CONSTRAINT ecmwf_actual_coordinate_pkey PRIMARY KEY (latitude, longitude)
)
DISTSTYLE ALL;


CREATE TABLE dms.ecmwf_forecast_coordinate (
    latitude DECIMAL(5, 2) NOT NULL,
    longitude DECIMAL(5, 2) NOT NULL,
    CONSTRAINT ecmwf_forecast_coordinate_pkey PRIMARY KEY (latitude, longitude)
)
DISTSTYLE ALL;


CREATE TABLE dms.date_time (
    date_time TIMESTAMP NOT NULL,
    "year" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "day" INTEGER NOT NULL,
    "hour" INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    CONSTRAINT date_time_pkey PRIMARY KEY (date_time)
)
DISTSTYLE ALL;


CREATE TABLE dms."date" (
    "date" DATE NOT NULL,
    "year" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "day" INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    CONSTRAINT date_pkey PRIMARY KEY ("date")
)
DISTSTYLE ALL;

