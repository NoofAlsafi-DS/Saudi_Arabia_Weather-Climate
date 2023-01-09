-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q18:  Count the average for air temperature, MIN air temperature, and MAX air temperature 
--        in each air temperature categories. present the result by each Season.

-- generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE Season_name, air_temperature, air_temperature_categories;

-- Group Generate_data
Data_group = GROUP Generate_data BY (Season_name,air_temperature_categories);

-- Count the AVG, MIN, and MAX of air_temperature in each categories
Year_Season_clime = FOREACH Data_group GENERATE group, 
ROUND_TO(AVG(Generate_data.air_temperature),2),
MIN(Generate_data.air_temperature),
MAX(Generate_data.air_temperature);

-- Save the results into different datasets
STORE Year_Season_clime INTO 'Year_Season_clime';

-- Display the result on screen
DUMP Year_Season_clime;

/*
We see a diversity in the classification of Saudi weather between hot, cold, warm, and comfortable 
in all seasons of the year.

*/