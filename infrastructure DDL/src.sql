-- 1 шаг - загрузка данных в src
create table src.flights(
flight_year varchar(5) not NULL, 
flight_quarter varchar(5) not NULL, 
flight_month varchar(5) not NULL, 
flight_date varchar(30) not NULL, 
reporting_airline varchar(5) not null,
tail_number varchar(10),
flight_number varchar(10) not null,
origin varchar(5) not null,
destination varchar(5) not null,
crs_dep_time varchar(15) not null,
dep_time varchar(15),
dep_delay_min varchar(10),
cancelled float not null,
cancellation_code char(1),
air_time varchar(10),
distance varchar(10) not null,
weather_delay varchar(10),
loaded_ts timestamp NOT NULL DEFAULT now()
); -- works

\copy src.flights(flight_year, flight_quarter, flight_month, flight_date, reporting_airline, tail_number, flight_number, origin, destination, crs_dep_time, dep_time, dep_delay_min, cancelled, cancellation_code, air_time,distance, weather_delay) from '/Users/viktorz3/Desktop/dwh/!project/src data/T_ONTIME_REPORTING_05.csv' with delimiter ',' CSV HEADER;




create table src.weather(
icao_code varchar(10) default 'KJFK',
local_datetime varchar(25) not null,
air_temp varchar(10) not null,
p0_station_lvl varchar(10) NOT null,
p_sea_lvl varchar(10) NOT null,
humidity varchar(5) not null,
wind_direction varchar(100),
wind_speed varchar(5),
max_gust_speed varchar(10),
phenomena_observed varchar(50),
phenomena_significant varchar(50),
total_cloud_cover varchar(1000) not null,
visibility varchar(10) not null,
dewpoint_temp varchar(10),
loaded_ts timestamp NOT NULL DEFAULT now()
); --works

\copy src.weather(local_datetime, air_temp, p0_station_lvl, p_sea_lvl, humidity, wind_direction, wind_speed, max_gust_speed, phenomena_observed, phenomena_significant, total_cloud_cover, visibility, dewpoint_temp) from '/Users/viktorz3/Desktop/dwh/!project/src data/KJFK_weather_05_08.csv' with delimiter ';' CSV HEADER;





