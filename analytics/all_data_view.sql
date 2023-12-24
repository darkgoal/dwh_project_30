CREATE OR REPLACE VIEW mart.kdz_30_flights_full_data AS
	SELECT fd.airport_origin_dk, fd.airport_destination_dk, fd.weather_type_dk, fd.flight_scheduled_ts, 
	fd.flight_actual_time, fd.flight_number, fd.distance, fd.tail_number, fd.airline, fd.dep_delay_min,
	fd.cancelled, fd.cancellation_code, cci.reason, fd.t, fd.max_gws, fd.w_speed, fd.air_time, 
	wt.cold, wt.rain, wt.snow, wt.thunderstorm, wt.drizzle, wt.fog_mist,
	ia1.airport_origin_iata_id, ia2.airport_destination_iata_id
FROM
	mart.fact_departure fd 
		JOIN (SELECT weather_type_dk, cold, rain, snow, thunderstorm, drizzle, fog_mist FROM dds.weather_type) wt USING(weather_type_dk)
		INNER JOIN (SELECT dwh_dk, src_iata_id as airport_origin_iata_id FROM dwh.id_airport) ia1 ON fd.airport_origin_dk = ia1.dwh_dk
		INNER JOIN (SELECT dwh_dk, src_iata_id as airport_destination_iata_id FROM dwh.id_airport) ia2 ON fd.airport_destination_dk = ia2.dwh_dk
		INNER JOIN kdz_30_dds.cancellation_code_id cci USING(cancellation_code)
WHERE fd.author = '30' AND airport_origin_iata_id = 'JFK' or airport_destination_iata_id = 'JFK';
