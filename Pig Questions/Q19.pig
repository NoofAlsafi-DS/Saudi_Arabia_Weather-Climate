-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q19: If the station register poor high humidity levels, which mean 73% and more,
--      and visibility distance less than 4km, give the information about the climate.

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE  humidity_level, visibility_distance, Season_name, 
station_name, air_temperature, wind_speed_rate;

-- Filter the data to display only the data Conforming to the conditions
Filter_MIN = FILTER Generate_data BY (humidity_level matches '73% and higher') AND
(visibility_distance < 1000);

-- Arrange the results from the lowest to the highest visibility distance
Arrange_Results = ORDER Filter_MIN BY visibility_distance ASC;

-- Save the results into different datasets
STORE Arrange_Results INTO 'Arrange_Results';

-- Display the result on screen
DUMP Arrange_Results;

/*
For sure when the station registers poor high humidity levels that means the visibility distance 
will effect by decreasing. The result appears to clarify that this status occurs in the Summer 
and Fall seasons more than others.

*/