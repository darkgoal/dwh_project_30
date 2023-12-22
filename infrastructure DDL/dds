-- создание таблицы flights уровня DDS

CREATE TABLE kdz_30_dds.flights (
 year int NULL,
 quarter int NULL,
 month int NULL,
 flight_scheduled_date date NULL,
 flight_actual_date date NULL,
 flight_dep_scheduled_ts timestamp NOT NULL,
 flight_dep_actual_ts timestamp NULL,
 report_airline varchar(10) NOT NULL,
 tail_number varchar(10) NOT NULL,
 flight_number_reporting_airline varchar(15) NOT NULL,
 airport_origin_dk int NULL, 
 origin_code varchar(5) null,
 airport_dest_dk int NULL,  
 dest_code varchar(5) null,
 dep_delay_minutes float NULL,
 cancelled int NOT NULL,
 cancellation_code char(1) NULL,
 weather_delay float NULL,
 air_time float NULL,
 distance float NULL,
 loaded_ts timestamp default(now()),
 CONSTRAINT flights_pk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);


-- инициация - первая загрузка flights в DDS

create table if not exists kdz_30_etl.dds_load_flights_02 as
select
 flight_year as year, 
 flight_quarter as quarter, 
 flight_month as month, 
 flight_date as flight_scheduled_date,
 (CASE WHEN (cancelled = 0) THEN DATE(date_add(flight_date, ((dep_delay_min::varchar(10)  ' '  ' min')::interval))::varchar(30)) ELSE NULL END)  as flight_actual_date,
 (flight_date::varchar(25)  ' '  crs_dep_time::varchar(25))::timestamp as flight_dep_scheduled_ts,
 (CASE WHEN (cancelled = 0) THEN (date_add((flight_date::varchar(25)  ' '  crs_dep_time::varchar(25))::timestamp, (dep_delay_min::varchar(10)  ' '  'min')::interval)) ELSE NULL END) as flight_dep_actual_ts,
 reporting_airline as report_airline,
 (CASE WHEN (flight_number IS NULL) THEN ' ' ELSE flight_number END) as tail_number,
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
from kdz_30_staging.flights;




-- создание таблицы weather уровня DDS

CREATE TABLE kdz_30_dds.weather (
 airport_dk int NOT NULL, 
 weather_type_dk char(6) NOT NULL, -- постоянный ключ типа погоды. заполняется по формуле
 cold smallint default(0),
 rain smallint default(0),
 snow smallint default(0),
 thunderstorm smallint default(0),
 drizzle smallint default(0),
 fog_mist smallint default(0),
 t int NULL,
 max_gws int NULL,
 w_speed int NULL,
 date_start timestamp NOT NULL,
 date_end timestamp NOT NULL default('3000-01-01'::timestamp),
 loaded_ts timestamp default(now()),
 PRIMARY KEY (airport_dk, date_start)
);


-- инициация - первая загрузка weather в DDS

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
from kdz_30_staging.weather;
