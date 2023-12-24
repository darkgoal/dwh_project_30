-- создание таблицы фактов для основной витрины данных

CREATE TABLE kdz_30_mart.fact_departure (
	airport_origin_dk int4 NOT NULL,
	airport_destination_dk int4 NOT NULL,
	weather_type_dk bpchar(6) NULL,
	flight_scheduled_ts timestamp NOT NULL,
	flight_actual_time timestamp NULL,
	flight_number varchar(15) NOT NULL,
	distance float8 NULL,
	tail_number varchar(20) NOT NULL,
	airline varchar(100) NOT NULL,
	dep_delay_min int4 NULL,
	cancelled int2 NOT NULL,
	cancellation_code bpchar(1) NULL,
	t float8 NULL,
	max_gws int4 NULL,
	w_speed int4 NULL,
	air_time int4 NULL,
	author varchar(40) NULL,
	loaded_ts timestamp NOT NULL DEFAULT (NOW()),
	CONSTRAINT fact_departure_pk PRIMARY KEY (flight_scheduled_ts, flight_number, airport_origin_dk, airport_destination_dk)
);




-- инициализирующая загрузка

create table if not exists kdz_30_etl.fact_download_02
as select 
	f.airport_origin_dk,
	f.airport_dest_dk as airport_destination_dk,
	w.weather_type_dk ,
	f.flight_dep_scheduled_ts as flight_scheduled_ts,
	f.flight_dep_actual_ts as flight_actual_time,
	f.flight_number_reporting_airline as flight_number,
	f.distance,
	f.tail_number,
	f.report_airline as airline,
	f.dep_delay_minutes as dep_delay_min,
	f.cancelled,
	f.cancellation_code,
	w.t,
	w.max_gws,
	w.w_speed,
	f.air_time,
	'30' as author 
from kdz_30_dds.flights as f
inner join kdz_30_dds.weather as w
	on f.flight_dep_scheduled_ts >= w.date_start and f.flight_dep_scheduled_ts < w.date_end
	and (f.airport_origin_dk = w.airport_dk or f.airport_dest_dk = w.airport_dk);
