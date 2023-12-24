
-- 2 шаг - перенос данных src -> staging
-- процедура загрузки данных в staging будет инкрементной, тк уже записанные данные о рейсах 
-- могут меняться (мы так решили и точка)
-- более того, такая процедура будет более быстрой и менее ресурсоемкой




-- 2.1 -- создаем таблицу с маркером последнего обработанного значения

create table if not exists kdz_30_etl.load_flights_03(
loaded_ts timestamp not null primary key
-- маркер последнего обработанного значения
); 



-- 2.2 -- определение границ самых свежих данных в src
drop table if exists kdz_30_etl.load_flights_01;

create table if not exists kdz_30_etl.load_flights_01 as
select
	min(loaded_ts) as ts1,
	max(loaded_ts) as ts2
from kdz_30_src.flights
where loaded_ts >= coalesce(
	(select max(loaded_ts) from kdz_30_etl.load_flights_03), 
'1970-01-01');



-- 2.3 -- чтение сырых данных (снимок), которые раньше НЕ были обработаны

create table if not exists kdz_30_etl.load_flights_02 as
select distinct
	cast(flight_year as int) as flight_year, 
	cast(flight_quarter as int) as flight_quarter, 
	cast(flight_month as int) as flight_month , 
	to_date(flight_date, 'MM/DD/YYYY') as flight_date, 
	reporting_airline,
	tail_number,
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
from kdz_30_src.flights, kdz_30_etl.load_flights_01
where loaded_ts > ts1 and loaded_ts <= ts2; -- ограничение на обработку 


-- 2.4 запись в целевую таблицу в режиме upsert
insert into kdz_30_staging.flights(
	flight_year, 
	flight_quarter, 
	flight_month , 
	flight_date, 
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	destination,
	crs_dep_time,
	dep_time,
	dep_delay_min,
	cancelled,
	cancellation_code,
	air_time,
	distance,
	weather_delay
	)
select 
	flight_year, 
	flight_quarter, 
	flight_month , 
	flight_date, 
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	destination,
	crs_dep_time,
	dep_time,
	dep_delay_min,
	cancelled,
	cancellation_code,
	air_time,
	distance,
	weather_delay
from kdz_30_etl.load_flights_02
on conflict on constraint flights_pkey do update
set (tail_number, destination, dep_time, dep_delay_min, cancelled, cancellation_code, air_time, distance, weather_delay) = 
	(EXCLUDED.tail_number, EXCLUDED.destination, EXCLUDED.dep_time, EXCLUDED.dep_delay_min, 
	EXCLUDED.cancelled, EXCLUDED.cancellation_code, EXCLUDED.air_time, EXCLUDED.distance, EXCLUDED.weather_delay);

-- мы предполагаем, что в настоящей системе (при загрузке не из бд BTS) в таблице могут появлятся в том числе и запланированные рейсы,
-- поэтому, следует учесть вероятность изменений данных задним числом



-- 2.5 обновление последней известной метки loaded_ts
delete from kdz_30_etl.load_flights_03 
where exists (select 1 from kdz_30_etl.load_flights_01);

insert into kdz_30_etl.load_flights_03 (loaded_ts)
select ts2
from kdz_30_etl.load_flights_01
where exists (select 1 from kdz_30_etl.load_flights_01);


-- конец загрузки flights







-- шаг 2 (ETL 1) для weather

-- 2.1 -- создаем таблицу с маркером последнего обработанного значения
create table if not exists kdz_30_etl.load_weather_03(
loaded_ts timestamp not null primary key
-- маркер последнего обработанного значения
); 




-- 2.2 -- определение границ самых свежих данных в src
drop table if exists kdz_30_etl.load_weather_01;

create table if not exists kdz_30_etl.load_weather_01 as
select
	min(loaded_ts) as ts1,
	max(loaded_ts) as ts2
from kdz_30_src.flights
where loaded_ts >= coalesce((select max(loaded_ts) 
from kdz_30_etl.load_weather_03), '1970-01-01');



-- 2.3 -- чтение сырых данных (снимок), которые раньше НЕ были обработаны

drop table if exists kdz_30_etl.load_weather_02;

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
from kdz_30_src.weather, kdz_30_etl.load_weather_01
where loaded_ts > ts1 and loaded_ts <= ts2; -- ограничение на обработку 



-- 2.4 запись в целевую таблицу в режиме upsert
insert into kdz_30_staging.weather(
	icao_code,
	local_datetime,
	air_temp,
	p0_station_lvl,
	p_sea_lvl,
	humidity,
	wind_direction,
	wind_speed,
	max_gust_speed,
	phenomena_observed,
	phenomena_significant,
	total_cloud_cover,
	visibility,
	dewpoint_temp
) select
	icao_code,
	local_datetime,
	air_temp,
	p0_station_lvl,
	p_sea_lvl,
	humidity,
	wind_direction,
	wind_speed,
	max_gust_speed,
	phenomena_observed,
	phenomena_significant,
	total_cloud_cover,
	visibility,
	dewpoint_temp
from kdz_30_etl.load_weather_02
on conflict on constraint weather_pkey do nothing;




-- 2.5 обновление последней известной метки loaded_ts
delete from kdz_30_etl.load_weather_03 
where exists (select 1 from kdz_30_etl.load_weather_01);

insert into kdz_30_etl.load_weather_03 (loaded_ts)
select ts2
from kdz_30_etl.load_weather_01 
where exists (select 1 from kdz_30_etl.load_weather_01);

-- конец etl1 для weather
