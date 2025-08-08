CREATE TABLE IF NOT EXISTS public.weather_facts_daily (
  country VARCHAR(5),
  city_name VARCHAR(255),
  record_date DATE,
  avg_temp NUMERIC,
  min_temp NUMERIC,
  max_temp NUMERIC,
  avg_humidity NUMERIC,
  avg_wind_speed NUMERIC,
  avg_visibility NUMERIC,
  load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (country, city_name, record_date)
);

INSERT INTO public.weather_facts_daily (
  country,
  city_name,
  record_date,
  avg_temp,
  min_temp,
  max_temp,
  avg_humidity,
  avg_wind_speed,
  avg_visibility,
  load_timestamp
)
SELECT
  rw.country,
  rw.city_name,
  rw.data_received_timestamp_utc::DATE AS record_date,
  TRUNC(AVG(rw.temperature),1) AS avg_temp,
  MIN(rw.temp_min) AS min_temp,
  MAX(rw.temp_max) AS max_temp,
  CAST(AVG(rw.humidity) AS INTEGER) AS avg_humidity,
  TRUNC(AVG(rw.wind_speed), 2) AS avg_wind_speed,
  CAST(AVG(rw.visibility) AS INTEGER) AS avg_visibility,
  DATE_TRUNC('second', CURRENT_TIMESTAMP) AS load_timestamp
FROM
  staging.raw_weather_data rw
WHERE
  rw.data_received_timestamp_utc::DATE = '{{ ds }}'::DATE
GROUP BY
  rw.country,
  rw.city_name,
  rw.data_received_timestamp_utc::DATE
ON CONFLICT (city_name, record_date) DO UPDATE SET
  avg_temp = EXCLUDED.avg_temp,
  min_temp = EXCLUDED.min_temp,
  max_temp = EXCLUDED.max_temp,
  avg_humidity = EXCLUDED.avg_humidity,
  avg_wind_speed = EXCLUDED.avg_wind_speed,
  avg_visibility = EXCLUDED.avg_visibility,
  load_timestamp = CURRENT_TIMESTAMP;