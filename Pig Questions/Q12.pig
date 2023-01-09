-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q12: Which station registers as a highest wind speed rate in the last 6 years?
--     present the date, time, and any information that can affect wind speed rate

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE observation_date, station_name, elevation,
Season_name,air_temperature, wind_direction_angle, wind_type, wind_speed_rate;

-- Group Generate_data
Data_group = GROUP Generate_data ALL;

-- Count the MIN wind speed rate for each station in the last 6 years
Count_MAX = FOREACH Data_group GENERATE MAX(Generate_data.wind_speed_rate) as wind_speed_rate;

-- Filter the data to deasplay only a highest wind speed rate in the last 6 years and related informations
Filter_MAX = FILTER Generate_data BY (wind_speed_rate == (float)Count_MAX.wind_speed_rate);

-- Save the results into different datasets
STORE Filter_MAX INTO 'Filter_MAX';

-- Display the result on screen
DUMP Filter_MAX;

/*
According to the statistics of the National Center of Meteorology, winds above 40 m/s are called hurricane winds, 
which may raise dust and reduce visibility, and this may cause a lot of flight disruptions or danger while driving.

*/
