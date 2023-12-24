-- ETL 4: dds -> mart.fact_departure


-- 4.1 маркер последней загрузки

create table if not exists kdz_30_etl.fact_download_03(
loaded_ts timestamp not null primary key
); 


-- 4.2 границы самых свежих данных
drop table if exists kdz_30_etl.fact_download_01;
-- проблем с потерей дат не будет, тк при join останутся все даты полетов
create table if not exists kdz_30_etl.fact_download_01 as
select
	min(loaded_ts) as ts1,
	max(loaded_ts) as ts2
from kdz_30_dds.flights
where loaded_ts >= coalesce((select max(loaded_ts) 
from kdz_30_etl.fact_download_03), '1970-01-01');


-- 4.3 чтение данных (снимок) с соединением с таблицей погоды по временному промежутку погоды и времени вылета
drop table if exists kdz_30_etl.fact_download_02;

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
from (kdz_30_dds.flights as f
inner join kdz_30_dds.weather as w
	on f.flight_dep_scheduled_ts >= w.date_start and f.flight_dep_scheduled_ts < w.date_end
	and (f.airport_origin_dk = w.airport_dk or f.airport_dest_dk = w.airport_dk)), kdz_30_etl.fact_download_01
where f.loaded_ts > ts1 and f.loaded_ts <= ts2;




-- 4.4 загрузка в таблицу фактов основной витрины данных
insert into mart.fact_departure (
	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	loaded_ts
	) select 
	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	now() as loaded_ts
from kdz_30_etl.fact_download_02
ON CONFLICT ON CONSTRAINT fact_departure_pk DO UPDATE
SET (weather_type_dk, flight_actual_time, author, loaded_ts) = (EXCLUDED.weather_type_dk, EXCLUDED.flight_actual_time, EXCLUDED.author, now());
-- эти замены в уже существующих строках мы делаем, если кто-то опять загрузил за нас наши данные и сделал это неправильно
