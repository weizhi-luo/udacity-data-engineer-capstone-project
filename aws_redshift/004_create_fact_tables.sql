CREATE TABLE fct.ecmwf_actual (
    latitude DECIMAL(5, 2) NOT NULL,
    longitude DECIMAL(5, 2) NOT NULL,
    value_date_time TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION, 
    u_wind_component DOUBLE PRECISION,
    v_wind_component DOUBLE PRECISION, 
    mean_sea_level_pressure DOUBLE PRECISION, 	
    surface_pressure DOUBLE PRECISION, 
    total_column_vertically_integrated_water_vapour DOUBLE PRECISION, 
    total_precipitation DOUBLE PRECISION, 
    skin_temperature DOUBLE PRECISION,
    FOREIGN KEY (latitude, longitude) REFERENCES dms.ecmwf_actual_coordinate(latitude, longitude),
    FOREIGN KEY (value_date_time) REFERENCES dms.date_time (date_time)
);
--
--
CREATE TABLE fct.ecmwf_forecast (
    latitude DECIMAL(5, 2) NOT NULL,
    longitude DECIMAL(5, 2) NOT NULL,
    forecast_date_time TIMESTAMP NOT NULL,
    value_date_time TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION, 
    u_wind_component DOUBLE PRECISION,
    v_wind_component DOUBLE PRECISION, 
    mean_sea_level_pressure DOUBLE PRECISION, 	
    surface_pressure DOUBLE PRECISION, 
    total_column_vertically_integrated_water_vapour DOUBLE PRECISION, 
    total_precipitation DOUBLE PRECISION, 
    skin_temperature DOUBLE PRECISION,
    FOREIGN KEY (latitude, longitude) REFERENCES dms.ecmwf_forecast_coordinate(latitude, longitude),
    FOREIGN KEY (value_date_time) REFERENCES dms.date_time (date_time)
)
DISTKEY(forecast_date_time);


CREATE TABLE fct.rail_service_performance (
    origin_location_id INTEGER NOT NULL, 
    destination_location_id INTEGER NOT NULL, 
    "date" DATE NOT NULL, 
    arrival_time TIME NOT NULL, 
    departure_time TIME NOT NULL, 
    train_service_operator_id INTEGER, 
    rid VARCHAR(30), 
    delayed BOOLEAN NOT NULL, 
    delay_tolerance_minute SMALLINT,
    FOREIGN KEY (origin_location_id) REFERENCES dms.station (id),
    FOREIGN KEY (destination_location_id) REFERENCES dms.station (id),
    FOREIGN KEY ("date") REFERENCES dms."date" ("date"),
    FOREIGN KEY (train_service_operator_id) REFERENCES dms.train_service_operator (id)
);
