-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q2: Count the highest and lowest air temperature for each station, and group by station name.

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, station_id, air_temperature;

-- Group Generate_data
Data_group = GROUP Generate_data BY (station_name);

-- Count the MAX and MIN of air temperature in each station
MAX_MIN_temperature = FOREACH Data_group GENERATE group, 
MAX(Generate_data.air_temperature) as max_air_tempreture,
MIN(Generate_data.air_temperature) as min_air_tempreture;

-- Save the results into different datasets
-- STORE MAX_MIN_temperature INTO 'MAX_MIN_temperature';

-- Display the result on screen
DUMP MAX_MIN_temperature;

/*
In the last six years, we can note that the stations' highest air temperature registers 
do not exceed 50 Degrees Celsius except for RAFHA, MAKKAH, ALAHSA, and QAISUMAH. 
On the other hand, there are many stations that register degrees less than 0 in minimum cases 
like GURIAT and TURAIF.

*/