-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q14: Count the average for air temperature, air temperature dew point, and visibility distance 
--      in each year for each season, save each season in each year in new dataset

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE Year, Season_name, air_temperature,
visibility_distance,air_temperature_dew_point;

-- Split data into different datasets based on the Season.
SPLIT Generate_data INTO Summer_2017 IF Season_name matches 'Summer' AND Year == 2017, 
Spring_2017 IF Season_name matches 'Spring' AND Year == 2017,
Winter_2017 IF Season_name matches 'Winter' AND Year == 2017, 
Fall_2017 IF Season_name matches 'Fall' AND Year == 2017,

Summer_2018 IF Season_name matches 'Summer' AND Year == 2018, 
Spring_2018 IF Season_name matches 'Spring' AND Year == 2018,
Winter_2018 IF Season_name matches 'Winter' AND Year == 2018, 
Fall_2018 IF Season_name matches 'Fall' AND Year == 2018,

Summer_2019 IF Season_name matches 'Summer' AND Year == 2019, 
Spring_2019 IF Season_name matches 'Spring' AND Year == 2019,
Winter_2019 IF Season_name matches 'Winter' AND Year == 2019, 
Fall_2019 IF Season_name matches 'Fall' AND Year == 2019,

Summer_2020 IF Season_name matches 'Summer' AND Year == 2020, 
Spring_2020 IF Season_name matches 'Spring' AND Year == 2020,
Winter_2020 IF Season_name matches 'Winter' AND Year == 2020, 
Fall_2020 IF Season_name matches 'Fall' AND Year == 2020,

Summer_2021 IF Season_name matches 'Summer' AND Year == 2021, 
Spring_2021 IF Season_name matches 'Spring' AND Year == 2021,
Winter_2021 IF Season_name matches 'Winter' AND Year == 2021, 
Fall_2021 IF Season_name matches 'Fall' AND Year == 2021,

Summer_2022 IF Season_name matches 'Summer' AND Year == 2022, 
Spring_2022 IF Season_name matches 'Spring' AND Year == 2022,
Winter_2022 IF Season_name matches 'Winter' AND Year == 2022, 
Fall_2022 IF Season_name matches 'Fall' AND Year == 2022;

-- Save the results into different datasets
STORE Summer_2017 INTO 'Summer_2017' USING PigStorage (',');
STORE Spring_2017 INTO 'Spring_2017' USING PigStorage (',');
STORE Winter_2017 INTO 'Winter_2017' USING PigStorage (',');
STORE Fall_2017 INTO 'Fall_2017' USING PigStorage (',');

STORE Summer_2018 INTO 'Summer_2018' USING PigStorage (',');
STORE Spring_2018 INTO 'Spring_2018' USING PigStorage (',');
STORE Winter_2018 INTO 'Winter_2018' USING PigStorage (',');
STORE Fall_2018 INTO 'Fall_2018' USING PigStorage (',');

STORE Summer_2019 INTO 'Summer_2019' USING PigStorage (',');
STORE Spring_2019 INTO 'Spring_2019' USING PigStorage (',');
STORE Winter_2019 INTO 'Winter_2019' USING PigStorage (',');
STORE Fall_2019 INTO 'Fall_2019' USING PigStorage (',');

STORE Summer_2020 INTO 'Summer_2020' USING PigStorage (',');
STORE Spring_2020 INTO 'Spring_2020' USING PigStorage (',');
STORE Winter_2020 INTO 'Winter_2020' USING PigStorage (',');
STORE Fall_2020 INTO 'Fall_2020' USING PigStorage (',');

STORE Summer_2021 INTO 'Summer_2021' USING PigStorage (',');
STORE Spring_2021 INTO 'Spring_2021' USING PigStorage (',');
STORE Winter_2021 INTO 'Winter_2021' USING PigStorage (',');
STORE Fall_2021 INTO 'Fall_2021' USING PigStorage (',');

STORE Summer_2022 INTO 'Summer_2022' USING PigStorage (',');
STORE Spring_2022 INTO 'Spring_2022' USING PigStorage (',');
STORE Winter_2022 INTO 'Winter_2022' USING PigStorage (',');
STORE Fall_2022 INTO 'Fall_2022' USING PigStorage (',');

-- Group Generate_data
Data_group = GROUP Generate_data BY (Year,Season_name);

-- Count the AVG of air_temperature in each station
Year_Season_clime = FOREACH Data_group GENERATE group, 
ROUND_TO(AVG(Generate_data.air_temperature),2),
ROUND_TO(AVG(Generate_data.visibility_distance),2),
ROUND_TO(AVG(Generate_data.air_temperature_dew_point),2);

-- Display the result on screen
DUMP Year_Season_clime;

/*
We note that in the years 2017 and 2018 the average visibility is lower than in other years, 
and this may give us an indication of an improvement in visibility after the launch of the green 
Riyadh and Saudi green initiatives, which greatly limits the dust receding, and thus an increase 
in the visibility range.

*/