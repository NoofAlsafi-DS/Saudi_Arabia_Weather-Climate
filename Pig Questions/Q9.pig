-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q9: Count the Average wind speed rate for each station, 
--     and display the wind_direction_angle, wind_type, and elevation.


-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, elevation, wind_speed_rate,
wind_direction_angle, wind_type ;

-- Group Generate_data
Data_group = GROUP Generate_data BY (elevation,station_name, wind_direction_angle, wind_type);

-- Count the AVG of wind speed rate in each station
Avg_wind_speed_rate = FOREACH Data_group GENERATE
ROUND_TO(AVG(Generate_data.wind_speed_rate),2) AS wind_speed_rate, group;

-- Arrange the results from the lowest to the highest Avg wind speed rate
Arrange_Results = ORDER Avg_wind_speed_rate BY * ASC;

-- Save the results into different datasets
STORE Arrange_Results INTO 'Arrange_Results';

-- Display the result on screen
DUMP Arrange_Results;

/*
There are many factors that affect wind speed, type, and angle, such as the different seasons, 
as well as the height of the region, in addition to its nature. However, we can note that 
the results propagate with an average wind speed of 6 m/s.

*/
