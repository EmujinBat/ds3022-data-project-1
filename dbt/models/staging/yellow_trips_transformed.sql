-- Transformed yellow trips model
-- Adds: trip_co2_kgs, avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year
-- Uses a real-time lookup from the vehicle_emissions table (vehicle_type = 'yellow_taxi')

SELECT
	yt.*,
	-- total CO2 in kilograms: (miles * grams_per_mile) / 1000
	(yt.trip_distance * ve.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,

	-- average miles per hour: distance / duration_in_hours
	yt.trip_distance
		/ NULLIF((datediff('second', yt.tpep_pickup_datetime, yt.tpep_dropoff_datetime) / 3600.0), 0) AS avg_mph,

	-- extract time parts from pickup timestamp
	EXTRACT(HOUR FROM yt.tpep_pickup_datetime) AS hour_of_day,
	EXTRACT(DAYOFWEEK FROM yt.tpep_pickup_datetime) AS day_of_week,
	EXTRACT(WEEK FROM yt.tpep_pickup_datetime) AS week_of_year,
	EXTRACT(MONTH FROM yt.tpep_pickup_datetime) AS month_of_year

FROM yellow_trips_clean yt
LEFT JOIN vehicle_emissions ve
	ON ve.vehicle_type = 'yellow_taxi'


