class LoadDimensionTables:
    LOAD_TRAIN_SERVICE_OPERATOR = """
        insert into dms.train_service_operator
        (name, code)
        select 'The Chiltern Railway Company', 'CH'
        union all
        select 'Govia Thameslink Railway', 'GN'
        union all
        select 'Govia Thameslink Railway', 'TL'
        union all
        select 'Govia Thameslink Railway', 'SN'
        union all
        select 'First MTR South Western Trains', 'SW'
    """

    LOAD_DATE = """
        insert into dms.date
        select distinct TO_DATE("date", 'YYYY-MM-DD') AS "date", 
        DATE_PART(year, TO_DATE("date", 'YYYY-MM-DD')) AS "year",
        DATE_PART(month, TO_DATE("date", 'YYYY-MM-DD')) AS "month",
        DATE_PART(day, TO_DATE("date", 'YYYY-MM-DD')) AS "day",
        DATE_PART(week, TO_DATE("date", 'YYYY-MM-DD')) AS "week",   
        DATE_PART(dayofweek, TO_DATE("date", 'YYYY-MM-DD')) AS day_of_week                       
        from stg.rail_service_performance
    """

    LOAD_DATE_TIME = """
        INSERT INTO dms.date_time (date_time, "year", "month", "day", "hour", "week", day_of_week)
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_actual
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_10u
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_10v
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_2t
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_msl
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_skt
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_sp
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_tcwv
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_00_tp
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_10u
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_10v
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_2t
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_msl
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_skt
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_sp
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_tcwv
        UNION
        SELECT DISTINCT TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS date_time, 
        EXTRACT(year FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "year",
        EXTRACT(month FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "month",
        EXTRACT(day FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "day",
        EXTRACT(hour FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "hour",  
        EXTRACT(week FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS "week",  
        EXTRACT(dayofweek FROM TO_TIMESTAMP(value_date_time, 'YYYY-MM-DD HH24:MI:SS.US')) AS day_of_week                    
        FROM stg.ecmwf_forecast_12_tp
    """

    LOAD_ECMWF_ACTUAL_COORDINATES = """
        INSERT INTO dms.ecmwf_actual_coordinate 
        (latitude, longitude)
        SELECT DISTINCT latitude, longitude              
        FROM stg.ecmwf_actual
    """

    LOAD_ECMWF_FORECAST_COORDINATES = """
        INSERT INTO dms.ecmwf_forecast_coordinate
        (latitude, longitude)
        SELECT DISTINCT latitude, longitude
        FROM stg.ecmwf_forecast_00_10u
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_00_10v
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_00_2t
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_00_msl
        UNION
        SELECT DISTINCT latitude, longitude                 
        FROM stg.ecmwf_forecast_00_skt
        UNION
        SELECT DISTINCT latitude, longitude                 
        FROM stg.ecmwf_forecast_00_sp
        UNION
        SELECT DISTINCT latitude, longitude                  
        FROM stg.ecmwf_forecast_00_tcwv
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_00_tp
        UNION
        SELECT DISTINCT latitude, longitude
        FROM stg.ecmwf_forecast_12_10u
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_12_10v
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_12_2t
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_12_msl
        UNION
        SELECT DISTINCT latitude, longitude                 
        FROM stg.ecmwf_forecast_12_skt
        UNION
        SELECT DISTINCT latitude, longitude                 
        FROM stg.ecmwf_forecast_12_sp
        UNION
        SELECT DISTINCT latitude, longitude                  
        FROM stg.ecmwf_forecast_12_tcwv
        UNION
        SELECT DISTINCT latitude, longitude               
        FROM stg.ecmwf_forecast_12_tp
    """


class LoadFactTables:
    LOAD_ECMWF_ACTUAL = """
        INSERT INTO fct.ecmwf_actual
        SELECT distinct c.latitude, c.longitude, dt.date_time AS value_date_time,
        t2m AS temperature, u10 AS u_wind_component, v10 AS v_wind_component, 
        msl AS mean_sea_level_pressure, sp AS surface_pressure, 
        tcwv AS total_column_vertically_integrated_water_vapour, 
        tp AS total_precipitation, skt AS skin_temperature
        FROM stg.ecmwf_actual a
        INNER JOIN dms.date_time dt
        ON TO_TIMESTAMP(a.value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') = dt.date_time
        INNER JOIN dms.ecmwf_actual_coordinate c
        ON c.latitude = a.latitude
        AND c.longitude = a.longitude
    """

    LOAD_ECMWF_FORECAST = """
        INSERT INTO fct.ecmwf_forecast
        (latitude, longitude, forecast_date_time, value_date_time, temperature, 
         u_wind_component, v_wind_component, mean_sea_level_pressure, surface_pressure, 
         total_column_vertically_integrated_water_vapour, total_precipitation, skin_temperature)
        SELECT c.latitude, c.longitude, 
        TO_TIMESTAMP(u.forecast_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS forecast_date_time,
        dt.date_time AS value_date_time, t2m AS temperature, u10 AS u_wind_component, 
        v10 AS v_wind_component, msl AS mean_sea_level_pressure, sp AS surface_pressure, 
        tcwv AS total_column_vertically_integrated_water_vapour, tp AS total_precipitation, 
        skt AS skin_temperature
        FROM stg.ecmwf_forecast_00_10u u
        INNER JOIN stg.ecmwf_forecast_00_10v v
        ON v.latitude = u.latitude
        AND v.longitude = u.longitude
        AND v.forecast_date_time = u.forecast_date_time
        AND v.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_2t t
        ON t.latitude = u.latitude
        AND t.longitude = u.longitude
        AND t.forecast_date_time = u.forecast_date_time
        AND t.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_msl msl
        ON msl.latitude = u.latitude
        AND msl.longitude = u.longitude
        AND msl.forecast_date_time = u.forecast_date_time
        AND msl.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_skt skt
        ON skt.latitude = u.latitude
        AND skt.longitude = u.longitude
        AND skt.forecast_date_time = u.forecast_date_time
        AND skt.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_sp sp
        ON sp.latitude = u.latitude
        AND sp.longitude = u.longitude
        AND sp.forecast_date_time = u.forecast_date_time
        AND sp.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_tcwv tcwv
        ON tcwv.latitude = u.latitude
        AND tcwv.longitude = u.longitude
        AND tcwv.forecast_date_time = u.forecast_date_time
        AND tcwv.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_00_tp tp
        ON tp.latitude = u.latitude
        AND tp.longitude = u.longitude
        AND tp.forecast_date_time = u.forecast_date_time
        AND tp.value_date_time = u.value_date_time
        INNER JOIN dms.ecmwf_forecast_coordinate c
        ON c.latitude = u.latitude
        AND c.longitude = u.longitude
        INNER JOIN dms.date_time dt
        ON TO_TIMESTAMP(u.value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') = dt.date_time
        UNION
        SELECT c.latitude, c.longitude, 
        TO_TIMESTAMP(u.forecast_date_time, 'YYYY-MM-DD HH24:MI:SS.US') AS forecast_date_time,
        dt.date_time AS value_date_time, t2m AS temperature, u10 AS u_wind_component, 
        v10 AS v_wind_component, msl AS mean_sea_level_pressure, sp AS surface_pressure, 
        tcwv AS total_column_vertically_integrated_water_vapour, tp AS total_precipitation, 
        skt AS skin_temperature
        FROM stg.ecmwf_forecast_12_10u u
        INNER JOIN stg.ecmwf_forecast_12_10v v
        ON v.latitude = u.latitude
        AND v.longitude = u.longitude
        AND v.forecast_date_time = u.forecast_date_time
        AND v.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_2t t
        ON t.latitude = u.latitude
        AND t.longitude = u.longitude
        AND t.forecast_date_time = u.forecast_date_time
        AND t.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_msl msl
        ON msl.latitude = u.latitude
        AND msl.longitude = u.longitude
        AND msl.forecast_date_time = u.forecast_date_time
        AND msl.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_skt skt
        ON skt.latitude = u.latitude
        AND skt.longitude = u.longitude
        AND skt.forecast_date_time = u.forecast_date_time
        AND skt.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_sp sp
        ON sp.latitude = u.latitude
        AND sp.longitude = u.longitude
        AND sp.forecast_date_time = u.forecast_date_time
        AND sp.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_tcwv tcwv
        ON tcwv.latitude = u.latitude
        AND tcwv.longitude = u.longitude
        AND tcwv.forecast_date_time = u.forecast_date_time
        AND tcwv.value_date_time = u.value_date_time
        INNER JOIN stg.ecmwf_forecast_12_tp tp
        ON tp.latitude = u.latitude
        AND tp.longitude = u.longitude
        AND tp.forecast_date_time = u.forecast_date_time
        AND tp.value_date_time = u.value_date_time
        INNER JOIN dms.ecmwf_forecast_coordinate c
        ON c.latitude = u.latitude
        AND c.longitude = u.longitude
        INNER JOIN dms.date_time dt
        ON TO_TIMESTAMP(u.value_date_time, 'YYYY-MM-DD HH24:MI:SS.US') = dt.date_time

    """

    LOAD_RAIL_SERVICE_PERFORMANCE = """
        INSERT INTO fct.rail_service_performance
        (origin_location_id, destination_location_id, "date",
         arrival_time, departure_time, train_service_operator_id,
         rid, delayed, delay_tolerance_minute)
        SELECT os.id AS origin_location_id,
        ds.id AS destination_location_id, dt."date", 
        CAST(p.arrival_time AS TIME) AS arrival_time, 
        CAST(p.departure_time AS TIME) AS departure_time,
        op.id AS train_service_operator_id, p.rid,
        p.delayed, NULL AS delayed_tolerance_minute 
        FROM stg.rail_service_performance p
        INNER JOIN dms.station os
        ON os.code = p.origin_location
        INNER JOIN dms.station ds
        ON ds.code = p.destination_location
        INNER JOIN dms.train_service_operator op
        ON op.code = p.operator_code
        INNER JOIN dms."date" dt
        ON dt."date" = TO_DATE(p."date", 'YYYY-MM-DD')
        WHERE delayed = False
        AND delay_tolerance_minute = 0
        UNION
        SELECT os.id AS origin_location_id,
        ds.id AS destination_location_id, dt."date", 
        CAST(p.arrival_time AS TIME) AS arrival_time, 
        CAST(p.departure_time AS TIME) AS departure_time,
        op.id AS train_service_operator_id, p.rid,
        True as delayed, p.delay_tolerance_minute AS delayed_tolerance_minute 
        FROM (
            SELECT origin_location, destination_location,
            date, arrival_time, departure_time, operator_code,
            rid, delay_tolerance_minute, 
            ROW_NUMBER() OVER (
                PARTITION BY origin_location, destination_location,
                date, arrival_time, departure_time, operator_code, 
                rid 
                ORDER BY delay_tolerance_minute DESC
            ) AS row_number 
            FROM stg.rail_service_performance 
            WHERE delayed = True
        ) p
        INNER JOIN dms.station os
        ON os.code = p.origin_location
        INNER JOIN dms.station ds
        ON ds.code = p.destination_location
        INNER JOIN dms.train_service_operator op
        ON op.code = p.operator_code
        INNER JOIN dms."date" dt
        ON dt."date" = TO_DATE(p."date", 'YYYY-MM-DD')
        WHERE row_number = 1
    """
