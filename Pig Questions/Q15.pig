-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q15: Count the avrage air temperature dew point air temperature and visibility distance
--      for each humidity level, and store it in new dataset based-on humidity level

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE humidity_level, air_temperature_dew_point, 
visibility_distance, air_temperature;

-- Split data into different datasets based on the Season.
SPLIT Generate_data INTO Poor_high_humidity_levels IF humidity_level matches '73% and higher', 
Fair_humidity_levels IF (humidity_level matches '62_72%'),
Maintain_your_healthy_levels IF (humidity_level matches'31_36%') OR (humidity_level matches'52_61%') OR
(humidity_level matches'37_43%') OR (humidity_level matches'44_51%'),
Fair_humidity_levels IF humidity_level matches '26_30%', 
Poor_low_humidity_levels IF humidity_level matches '25% and lower';

-- Save the results into different datasets
STORE Poor_high_humidity_levels INTO 'Poor_high_humidity_levels' USING PigStorage (',');
STORE Fair_humidity_levels INTO 'Fair_humidity_levels' USING PigStorage (',');
STORE Maintain_your_healthy_levels INTO 'Maintain_your_healthy_levels' USING PigStorage (',');
STORE Fair_humidity_levels INTO 'Fair_humidity_levels' USING PigStorage (',');
STORE Poor_low_humidity_levels INTO 'Poor_low_humidity_levels' USING PigStorage (',');

-- Group Generate data
Data_group = GROUP Generate_data BY (humidity_level);

-- Count the AVG of air temperature and air temperature dew point and visibility distance
Humidity_level_data = FOREACH Data_group GENERATE group, 
ROUND_TO(AVG(Generate_data.air_temperature),2),
ROUND_TO(AVG(Generate_data.air_temperature_dew_point),2),
ROUND_TO(AVG(Generate_data.visibility_distance),2);

-- Arrange the results from the lowest to the highest Humidity level
Arrange_Results = ORDER Humidity_level_data BY * ASC;

-- Display the result on screen
DUMP Arrange_Results;

/*
We note that when the humidity level increase, the average air temperature dew point, and air temperature 
increase also. On the other hand, the average visibility distance decreased.

*/
