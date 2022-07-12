CREATE TABLE stg.ecmwf_actual (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    value_date_time VARCHAR,
    t2m DOUBLE PRECISION,
    u10 DOUBLE PRECISION,
    v10 DOUBLE PRECISION,
    msl DOUBLE PRECISION,
    sp DOUBLE PRECISION,
    tcwv DOUBLE PRECISION,
    tp DOUBLE PRECISION,
    skt DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_2t (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    t2m DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_msl (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    msl DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_10v (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    v10 DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_10u (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    u10 DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_tp (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    tp DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_tcwv (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    tcwv DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_sp (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    sp DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_00_skt (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    skt DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_2t (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    t2m DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_msl (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    msl DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_10v (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    v10 DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_10u (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    u10 DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_tp (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    tp DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_tcwv (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    tcwv DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_sp (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    sp DOUBLE PRECISION
);

CREATE TABLE stg.ecmwf_forecast_12_skt (
    latitude DECIMAL(5, 2),
    longitude DECIMAL(5, 2),
    forecast_date_time VARCHAR,
    value_date_time VARCHAR,
    skt DOUBLE PRECISION
);

CREATE TABLE stg.rail_service_performance (
    origin_location VARCHAR(10),
    destination_location VARCHAR(10),
    date VARCHAR,
    arrival_time VARCHAR,
    departure_time VARCHAR,
    operator_code VARCHAR(10),
    rid VARCHAR(30),
    delay_tolerance_minute SMALLINT,
    delayed BOOLEAN
);


