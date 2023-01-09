-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q7: Display the elevation for each station in Saudi Arabia,
--     and display the highest and lowest air temperature if the  Season name is summer.

-- Apply filter data, to display only the data in Summer Season
Filter_data = FILTER SaudiWeather BY (Season_name matches 'Summer');

-- Generate the columns we need to handle with it.
Generate_data = FOREACH Filter_data GENERATE station_name, elevation, air_temperature;

-- Group Generate_data
Data_group = GROUP Generate_data BY (elevation, station_name);

-- Count the MAX and MIN of air temperature in each station
MAX_MIN_temperature = FOREACH Data_group GENERATE group,
MAX(Generate_data.air_temperature),
MIN(Generate_data.air_temperature);

-- Arrange the results from the lowest to the highest elevation
Arrange_Results = ORDER MAX_MIN_temperature BY * ASC;

-- Save the results into different datasets
STORE Arrange_Results INTO 'Arrange_Results';

-- Display the result on screen
DUMP Arrange_Results;

/*
We note how the elevation of the regions affects the temperatures in the summer, 
as whenever the altitude decreases below 500 m, we witness high temperatures that may reach 50 degrees Celsius, 
while in the same season we find that there are many sites with temperatures approaching 30 degrees Celsius, 
due to its elevation of more than 1,500 m above sea level.

*/