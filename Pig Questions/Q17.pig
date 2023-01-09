-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q17: Which station has an air temperature less than or equal 20 in summer season
--      and wind Normal type. present the station name and lowest air temperature in this conditions.

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, air_temperature, 
wind_speed_rate, wind_type, Season_name;

-- Filter the data to display only the data Conforming to the conditions
Filter_MIN = FILTER Generate_data BY (air_temperature <= 20) AND (Season_name matches 'Summer') AND
(wind_type matches 'Normal');

-- Group Generate_data
Data_group = GROUP Filter_MIN BY (station_name);

-- Generate the columns we need to handle with it.
Generate_data = FOREACH Data_group GENERATE group, MIN(Filter_MIN.air_temperature);

-- Save the results into different datasets
STORE Generate_data INTO 'Generate_data';

-- Display the result on screen
DUMP Generate_data;

/*
There are many regions in Saudi Arabia that record average air temperatures of less than 20 degrees Celsius 
and may reach 13 degrees Celsius, which is a tourist destination for many visitors.

*/