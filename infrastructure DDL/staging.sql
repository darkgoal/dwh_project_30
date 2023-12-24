-- for flights

CREATE TABLE kdz_30_staging.flights (
	flight_year int not NULL, 
	flight_quarter int not NULL, 
	flight_month int not NULL, 
	flight_date date not NULL, 
	reporting_airline varchar(5) not null,
	tail_number varchar(10),
	flight_number varchar(10) not null,
	origin varchar(5) not null,
	destination varchar(5) not null,
	crs_dep_time time not null,
	dep_time time,
	dep_delay_min float,
	cancelled int not null,
	cancellation_code char(1),
	air_time float,
	distance float not null,
	weather_delay float,
	loaded_ts timestamp default(now()),
	CONSTRAINT flights_pkey PRIMARY KEY (flight_date, flight_number, origin, destination, crs_dep_time)
);


-- initiation download - flights

create table if not exists kdz_30_etl.load_flights_02 as
select distinct
	cast(flight_year as int) as flight_year, 
	cast(flight_quarter as int) as flight_quarter, 
	cast(flight_month as int) as flight_month , 
	to_date(flight_date, 'MM/DD/YYYY') as flight_date, 
	reporting_airline,
	(case when tail_number is null then '' else tail_number end) as tail_number,
	flight_number,
	origin,
	destination,
	(select case when crs_dep_time = '2400' then '2359' else crs_dep_time end)::TIME as crs_dep_time,
	(select case when dep_time = '2400' then '2359' else dep_time end)::TIME as dep_time,
	cast(dep_delay_min as float) as dep_delay_min,
	cast(cancelled as int) as cancelled,
	cancellation_code,
	air_time::float as air_time,
	distance::float as distance,
	weather_delay::float as weather_delay
from kdz_30_src.flights;






-- for weather

CREATE TABLE kdz_30_staging.weather (
	icao_code varchar(10) NOT NULL,
	local_datetime timestamp NOT NULL,
	air_temp numeric(3, 1) NOT NULL,
	p0_station_lvl numeric(4, 1) NOT NULL,
	p_sea_lvl numeric(4, 1) NOT NULL,
	humidity int4 NOT NULL,
	wind_direction varchar(100) NULL,
	wind_speed int4 NULL,
	max_gust_speed int4 NULL,
	phenomena_observed varchar(100) NULL,
	phenomena_significant varchar(50) NULL,
	total_cloud_cover varchar(200) NOT NULL,
	visibility numeric(3, 1) NOT NULL,
	dewpoint_temp numeric(3, 1),
	loaded_ts timestamp NOT NULL DEFAULT now(),
	PRIMARY KEY (icao_code, local_datetime)
); 



-- initiation download - weather

create table if not exists kdz_30_etl.load_weather_02 as
select distinct
	icao_code,
	to_timestamp(local_datetime, 'DD:MM:YYYY HH24:MI') as local_datetime,
	air_temp::numeric(3, 1) as air_temp,
	p0_station_lvl::numeric(4, 1) as p0_station_lvl,
	p_sea_lvl::numeric(4, 1) as p_sea_lvl,
	humidity::int4 as humidity,
	wind_direction,
	wind_speed::int4 as wind_speed,
	max_gust_speed::int4 as max_gust_speed,
	phenomena_observed,
	phenomena_significant,
	total_cloud_cover,
	visibility::numeric(3, 1) as visibility,
	dewpoint_temp::numeric(3, 1) as dewpoint_temp
from kdz_30_src.weather;

