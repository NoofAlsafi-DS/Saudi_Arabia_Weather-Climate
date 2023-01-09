-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q10: Count the Average air temperature for each station in each Season.
--     Save each season information in new dataset

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, Season_name, air_temperature;

-- Split data into different datasets based on the Season.
SPLIT Generate_data INTO Summer IF Season_name matches 'Summer', 
Spring IF Season_name matches 'Spring',
Winter IF Season_name matches 'Winter', 
Fall IF Season_name matches 'Fall';

-- Save the results into different datasets
STORE Summer INTO 'Summer' USING PigStorage (',');
STORE Spring INTO 'Spring' USING PigStorage (',');
STORE Winter INTO 'Winter' USING PigStorage (',');
STORE Fall INTO 'Fall' USING PigStorage (',');

-- Group Generate_data
Data_group = GROUP Generate_data BY (Season_name, station_name);

-- Count the AVG air temperature for each station in each season
Count_AVG = FOREACH Data_group GENERATE group, ROUND_TO(AVG(Generate_data.air_temperature),2);

-- Display the result on screen
DUMP Count_AVG;

/*
The answer may help many entities that seek to organize events in different seasons of the year, 
such as the tourism and entertainment sector, as the average temperature in winter is 
between 29 to 10 degrees Celsius, in summer and spring between 38 to 24 degrees Celsius, 
on the other hand, Fall is between 40 and 19 degrees Celsius.

*/