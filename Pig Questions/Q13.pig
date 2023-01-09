-- Load the Dataset

SaudiWeather = LOAD 'SaudiWeather.csv' 
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
As ( Year:int, station_name:chararray, station_id:int, observation_date:chararray, 
latitude:float, longitude:float, elevation:float,
wind_direction_angle:int, wind_type:chararray, wind_speed_rate:float,
sky_cavok:chararray, visibility_distance:int, air_temperature:float, air_temperature_dew_point:int, 
GEOPOINT:chararray, Month:int, Day:int, Hour:int, Minute:int,  Season:int, Season_name:chararray, 
humidity_level:chararray, air_temperature_categories:chararray);

-- Q13: what is the climate status when the visibility distance less than 1 km. 
--      display the result for each observation date.

-- Filter the data to deasplay only he visibility distance less than 1 km
Filter_MIN = FILTER SaudiWeather BY (visibility_distance < 1000);

-- Generate the columns we need todisplay it.
climate_status_data = FOREACH Filter_MIN GENERATE visibility_distance, observation_date, station_name,
sky_cavok, wind_speed_rate, air_temperature, air_temperature_dew_point, humidity_level;

-- Arrange the results from the lowest to the highest visibility distance
Arrange_Results = ORDER climate_status_data BY visibility_distance ASC;

-- Save the results into different datasets
STORE Arrange_Results INTO 'climate_status_data';

-- Display the result on screen
DUMP Arrange_Results;

/*
We find that the Kingdom, due to its desert nature, may face a lot of dust, which may reduce the clarity of vision, 
as the vision range of less than 1 km is known as very bad.

*/
