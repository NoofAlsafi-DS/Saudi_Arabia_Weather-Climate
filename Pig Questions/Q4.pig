-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q4: Count the highest and lowest temperature air temperature for each station in each years.
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

-- Count the MAX and MIN of air temperature for stations in each Year
MAX_MIN_temperature = FOREACH Data_group GENERATE group, 
MAX(Generate_data.air_temperature) as max_air_tempreture,
MIN(Generate_data.air_temperature) as min_air_tempreture;

-- Display the result on screen
DUMP MAX_MIN_temperature;

/*
The results show us the extent of progress in the success of Vision 2030 in terms of improving 
the environment and atmospheric climate, as we find that temperatures have decreased over the years, 
after they reached 51 degrees Celsius, in the last year they became less than 50 degrees, 
and we also notice a decrease in degrees less than 0 degrees Celsius registered in many regions.

*/