-- Load the Dataset
SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q1: Count the average air temperature for each station, and group by station name.

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, station_id, air_temperature;

-- Group Generate_data
Data_group = GROUP Generate_data BY (station_name);

-- Count the AVG of air_temperature in each station
Avg_temperature = FOREACH Data_group GENERATE group, 
ROUND_TO(AVG(Generate_data.air_temperature),2);

-- Save the results into different datasets
-- STORE Avg_temperature INTO 'Avg_temperature';

-- Display the result on screen
DUMP Avg_temperature;

/*
We can note that all average temperatures in all stations give us a rate of fewer than 30 Degrees Celsius 
in the last six years except for MAKKAH regions and SHARURAH. 
When we see the rest of the stations we noted TURAIF gets the lowest average air temperature.
*/