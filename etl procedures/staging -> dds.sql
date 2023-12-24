-- ETL 2 - flights
-- эта процедура будет инкрементной

-- 3.1 маркер последней загрузки
create table if not exists kdz_30_etl.dds_load_flights_03(
loaded_ts timestamp not null primary key
);


-- 3.2 границы самых свежих данных в staging
drop table if exists kdz_30_etl.dds_load_flights_01;

create table if not exists kdz_30_etl.dds_load_flights_01 as
select
 min(loaded_ts) as ts1,
 max(loaded_ts) as ts2
from kdz_30_staging.flights
where loaded_ts >= coalesce((select max(loaded_ts) 
from kdz_30_etl.dds_load_flights_03), '1970-01-01');

-- 3.3 -- чтение сырых данных (снимок), которые раньше НЕ были обработаны
-- P.S. работает только после инициализирующей загрузки

drop table if exists kdz_30_etl.dds_load_flights_02;

create table if not exists kdz_30_etl.dds_load_flights_02 as
select
 flight_year as year, 
 flight_quarter as quarter, 
 flight_month as month, 
 flight_date as flight_scheduled_date,
 (CASE WHEN (cancelled = 0) THEN DATE(flight_date::timestamp + ('' || dep_delay_min || ' minutes')::interval) ELSE NULL END)  as flight_actual_date,	
 (flight_date::varchar(25) ||  ' ' ||  crs_dep_time::varchar(25))::timestamp as flight_dep_scheduled_ts,
 (CASE WHEN (cancelled = 0) THEN (flight_date::varchar(30) || ' ' || crs_dep_time::varchar(25))::timestamp + ('' || dep_delay_min || ' minutes')::interval ELSE NULL END) as flight_dep_actual_ts,
 reporting_airline as report_airline,
 tail_number,
 flight_number as flight_number_reporting_airline,
 (select dwh_dk from dwh.id_airport where src_iata_id = origin) as airport_origin_dk,
 origin as origin_code,
 (select dwh_dk from dwh.id_airport where src_iata_id = destination) as airport_dest_dk,
 destination as dest_code,
 dep_delay_min as dep_delay_minutes,
 cancelled,
 cancellation_code,
 weather_delay,
 air_time,
 distance
from kdz_30_staging.flights, kdz_30_etl.dds_load_flights_01
where loaded_ts > ts1 and loaded_ts <= ts2;

-- 3.4 -- загрузка в dds
insert into kdz_30_dds.flights(
 year, 
 quarter, 
 month, 
 flight_scheduled_date,
 flight_actual_date,
 flight_dep_scheduled_ts,
 flight_dep_actual_ts,
 report_airline,
 tail_number,
 flight_number_reporting_airline,
 airport_origin_dk,
 origin_code,
 airport_dest_dk,
 dest_code,
 dep_delay_minutes,
 cancelled,
 cancellation_code,
 weather_delay,
 air_time,
 distance
 ) select 
 year, 
 quarter, 
 month, 
 flight_scheduled_date,
 flight_actual_date,
 flight_dep_scheduled_ts,
 flight_dep_actual_ts,
 report_airline,
 tail_number,
 flight_number_reporting_airline,
 airport_origin_dk,
 origin_code,
 airport_dest_dk,
 dest_code,
 dep_delay_minutes,
 cancelled,
 cancellation_code,
 weather_delay,
 air_time,
 distance
from kdz_30_etl.dds_load_flights_02
on conflict on constraint flights_pk do update
set (tail_number, flight_dep_actual_ts, dep_delay_minutes, cancelled, cancellation_code, air_time, distance, weather_delay, loaded_ts) = 
	(EXCLUDED.tail_number, EXCLUDED.flight_dep_actual_ts, EXCLUDED.dep_delay_minutes, 
	EXCLUDED.cancelled, EXCLUDED.cancellation_code, EXCLUDED.air_time, EXCLUDED.distance, EXCLUDED.weather_delay, now());


-- 3.5 обновление последней известной метки loaded_ts

delete from kdz_30_etl.dds_load_flights_03 
where exists (select 1 from kdz_30_etl.dds_load_flights_01);

insert into kdz_30_etl.dds_load_flights_03 (loaded_ts)
select ts2
from kdz_30_etl.dds_load_flights_01
where exists (select 1 from kdz_30_etl.dds_load_flights_01);

-- конец etl 2 для данных о полетах




-- ETL 2 - weather
-- эта процедура тоже инкрементная + SCD 2

-- 3.1 маркер последней загрузки

create table if not exists kdz_30_etl.dds_load_weather_03(
loaded_ts timestamp not null primary key
);

-- 3.2 границы самых свежих данных в staging
drop table if exists kdz_30_etl.dds_load_weather_01;

create table if not exists kdz_30_etl.dds_load_weather_01 as
select
 min(loaded_ts) as ts1,
 max(loaded_ts) as ts2
from staging.weather
where loaded_ts >= coalesce((select max(loaded_ts) 
from kdz_30_etl.dds_load_weather_i_03), '1970-01-01');

-- 3.3 - снимок новых данных (с доп. обработкой)

drop table if exists kdz_30_etl.dds_load_weather_02;

create table if not exists kdz_30_etl.dds_load_weather_02 as
select
 (select dwh_dk from dwh.id_airport where src_iata_id = 'JFK') as airport_dk, -- постоянный ключ аэропорта. нужно взять из таблицы аэропортов
  
 ((CASE WHEN (air_temp < 0) THEN 1 ELSE 0 END)::char 
   || (CASE WHEN ((phenomena_observed LIKE '%rain%') or (phenomena_significant LIKE '%rain%')) THEN 1 ELSE 0 END)::char
   || (CASE WHEN ((phenomena_observed LIKE '%snow%') or (phenomena_significant LIKE '%snow%')) THEN 1 ELSE 0 END)::char
   || (CASE WHEN ((phenomena_observed LIKE '%thunderstorm%') or (phenomena_significant LIKE '%thunderstorm%')) THEN 1 ELSE 0 END)::char
   || (CASE WHEN ((phenomena_observed LIKE '%drizzle%') or (phenomena_significant LIKE '%drizzle%')) THEN 1 ELSE 0 END)::char
   || (CASE WHEN (((phenomena_observed LIKE '%fog%') or (phenomena_significant LIKE '%fog%')) or ((phenomena_observed LIKE '%mist%') or (phenomena_significant LIKE '%mist%'))) THEN 1 ELSE 0 END)::char) 
 as weather_type_dk, -- постоянный ключ типа погоды. заполняется по формуле
  
 (CASE WHEN (air_temp < 0) THEN 1 ELSE 0 END) as cold,
 (CASE WHEN ((phenomena_observed LIKE '%rain%') or (phenomena_significant LIKE '%rain%')) THEN 1 ELSE 0 END) as rain,
 (CASE WHEN ((phenomena_observed LIKE '%snow%') or (phenomena_significant LIKE '%snow%')) THEN 1 ELSE 0 END) as snow,
 (CASE WHEN ((phenomena_observed LIKE '%thunderstorm%') or (phenomena_significant LIKE '%thunderstorm%')) THEN 1 ELSE 0 END) as thunderstorm,
 (CASE WHEN ((phenomena_observed LIKE '%drizzle%') or (phenomena_significant LIKE '%drizzle%')) THEN 1 ELSE 0 END) as drizzle,
 (CASE WHEN (((phenomena_observed LIKE '%fog%') or (phenomena_significant LIKE '%fog%')) or ((phenomena_observed LIKE '%mist%') or (phenomena_significant LIKE '%mist%'))) THEN 1 ELSE 0 END) as fog_mist,
 air_temp as t,
 max_gust_speed as max_gws,
 wind_speed as w_speed,
 local_datetime as date_start,
 coalesce(lead(local_datetime) over (order by local_datetime), '3000-01-01'::timestamp) as date_end
from kdz_30_staging.weather , kdz_30_etl.dds_load_weather_01
where loaded_ts > ts1 and loaded_ts <= ts2;

-- 3.4 обновление последнего date_end в старых данных на первый date_start из новых данных

update kdz_30_dds.weather 
set date_end = (select date_start 
 from etl.dds_load_weather_02
 order by date_start ASC
 LIMIT 1)
where date_end=('3000-01-01'::timestamp);

-- 3.5 загрузка в dds

insert into kdz_30_dds.weather(
 airport_dk, 
 weather_type_dk, 
 cold,
 rain,
 snow,
 thunderstorm,
 drizzle,
 fog_mist,
 t,
 max_gws,
 w_speed,
 date_start,
 date_end
) select
 airport_dk, 
 weather_type_dk, 
 cold,
 rain,
 snow,
 thunderstorm,
 drizzle,
 fog_mist,
 t,
 max_gws,
 w_speed,
 date_start,
 date_end
from kdz_30_etl.dds_load_weather_02;

-- 3.6 обновление последней известной метки loaded_ts

delete from kdz_30_etl.dds_load_flights_i_03 
where exists (select 1 from kdz_30_etl.dds_load_flights_i_01);

insert into kdz_30_etl.dds_load_flights_i_03 (loaded_ts)
select ts2
from kdz_30_etl.dds_load_flights_i_01
where exists (select 1 from kdz_30_etl.dds_load_flights_i_01);

-- конец etl 2 для данных погоды




-- тест для функции закрытия date_end
-- после первой загрузки данных и нашего src 

truncate table kdz_30_etl.dds_load_weather_02;

INSERT INTO kdz_30_etl.dds_load_weather_02
(airport_dk, weather_type_dk, cold, rain, snow, thunderstorm, drizzle, fog_mist, t, max_gws, w_speed, date_start, date_end)
VALUES(878, '111111', 1, 1, 1, 1, 1, 1, 1, 25, 100, '2021-09-01 23:51:00.000'::timestamp, '3000-01-01'::timestamp);

-- запускаем

update kdz_30_dds.weather 
set date_end = (select date_start 
 from kdz_30_etl.dds_load_weather_02
 order by date_start ASC
 LIMIT 1)
where date_end=('3000-01-01'::timestamp);

-- проверяем, закрылся ли date_end в последней старой
-- P.S. тест проведен успешно
-- можем делать insert новых данных в dds
