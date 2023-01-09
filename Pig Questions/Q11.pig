-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q11: Count the MAX and MIN air temperature for each station in each Season.
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

-- Count the MAX and MIN air temperature for each station in each season
Count_MAX_MIN = FOREACH Data_group GENERATE group, 
MAX(Generate_data.air_temperature),
MIN(Generate_data.air_temperature);

-- Display the result on screen
DUMP Count_MAX_MIN;

/*
This data serves many people who would like to spend a vacation in one of the seasons in the Kingdom, 
as the highest and lowest temperature in the season helps us choose the area that suits the activities 
that we want to attend, in the winter season the temperatures range from 40 to - 6 degrees Celsius, 
in the summer it ranges between 50 to 19 degrees Celsius, while in the spring we find degrees that are 
characterized by moderation between 48 to 13 degrees Celsius, also the Fall season records close degrees of it.

*/
