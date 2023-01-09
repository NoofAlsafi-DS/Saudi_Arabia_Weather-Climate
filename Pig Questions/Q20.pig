-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q20: what is the avrage air temperature if the air temperature categories comfortable and season not Summer,
--      and not Winter with wind type Normal for stations elevation less than 1 km

-- Generate the columns we need to handle with it.
Generate_data = FOREACH SaudiWeather GENERATE station_name, elevation,
Season_name, air_temperature, air_temperature_categories,wind_type;

-- Filter the data to display only the data Conforming to the conditions
Filter_MIN = FILTER Generate_data BY  (air_temperature_categories matches 'comfortable')  AND
NOT(Season_name matches 'Summer') AND NOT(Season_name matches 'Winter') AND
(wind_type matches 'Normal') AND (elevation < 1000) ;

-- Group Generate_data
Data_group = GROUP Filter_MIN BY (station_name, elevation);

-- Count the AVG of air temperature in each station_name
Year_Season_clime = FOREACH Data_group GENERATE 
ROUND_TO(AVG(Filter_MIN.air_temperature),2), group;

-- Save the results into different datasets
STORE Year_Season_clime INTO 'Year_Season_clime';

-- Display the result on screen
DUMP Year_Season_clime;

/*
While the altitude of the regions may affect the improvement of the average temperatures, 
we may find that the regions that rise from sea level by less than 1000 m record an average temperature 
of about 20 degrees Celsius in the spring and autumn seasons with Normal wind type.
*/
