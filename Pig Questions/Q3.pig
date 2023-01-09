-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q3: Count the average air temperature for each station in each years, 
--     and save each year in a different datasets.

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE Year, station_name, air_temperature;

-- Split data into different datasets based on the Year.
SPLIT Generate_data INTO Year_2017 IF Year == 2017, Year_2018 IF Year == 2018,
Year_2019 IF Year == 2019, Year_2020 IF Year == 2020, Year_2021 IF Year == 2021,
Year_2022 IF Year == 2022;

-- Save the results into different datasets
STORE Year_2017 INTO 'Year_2017' USING PigStorage (',');
STORE Year_2018 INTO 'Year_2018' USING PigStorage (',');
STORE Year_2019 INTO 'Year_2019' USING PigStorage (',');
STORE Year_2020 INTO 'Year_2020' USING PigStorage (',');
STORE Year_2021 INTO 'Year_2021' USING PigStorage (',');
STORE Year_2022 INTO 'Year_2022' USING PigStorage (',');

-- Group Generate_data
Data_group = GROUP Generate_data BY (Year,station_name);

-- Count the AVG of air temperature in each station
Avg_temperature = FOREACH Data_group GENERATE group, 
ROUND_TO(AVG(Generate_data.air_temperature),2);

-- Display the result on screen
DUMP Avg_temperature;

/*
We note that the average temperatures during the past six years do not exceed 30 degrees Celsius, 
as the average degrees range between 18-29 degrees Celsius almost in all regions, but on the opposite, 
we find that MAKKAH reaches or slightly exceeds 30 degrees Celsius every year.

*/
