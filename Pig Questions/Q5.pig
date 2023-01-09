-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q5: Which station registers as a highest air temperature in the last 6 years?
--     present the date, time, and any information that can affect air temperature

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE observation_date, station_name, elevation,
air_temperature_dew_point, humidity_level,Season_name,air_temperature_categories,air_temperature;

-- Group Generate_data
Data_group = GROUP Generate_data ALL;

-- Count the MAX air temperature for each station in the last 6 years
Count_MAX = FOREACH Data_group GENERATE MAX(Generate_data.air_temperature) as air_temperature;

-- Filter the data to deasplay only a highest air temperature in the last 6 years and related informations
Filter_MAX = FILTER Generate_data BY (air_temperature == (float)Count_MAX.air_temperature);

-- Save the results into different datasets
STORE Filter_MAX INTO 'Filter_MAX';

-- Display the result on screen
DUMP Filter_MAX;

/*
In RAFHA, the desert climate prevails, with hot dry summers and dry, cold winters. 
The vegetation cover is only a reflection of the amount of rain because of that we can note how air temperature 
effecting on RAFHA during the summer season .

*/
