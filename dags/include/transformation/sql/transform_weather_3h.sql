CREATE TABLE IF NOT EXISTS public.weather_facts_every_3h(
  country VARCHAR(5),
  city_name VARCHAR(255),
  record_date TIMESTAMP WITH TIME ZONE,
  avg_temp NUMERIC,
  avg_humidity NUMERIC,
  maj_weather_description VARCHAR(255),
  avg_wind_speed NUMERIC,
  avg_visibility NUMERIC,
  load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  is_dark INTEGER, --0 is Daylight, 1 is Dawn, 2 is Twilight, 3 is completely Dark
  PRIMARY KEY (country, city_name, record_date)
);

WITH avg_metrics AS (
  SELECT
    rw.country,
    rw.city_name,
    DATE_TRUNC('hour', data_received_timestamp_utc)
              - ((EXTRACT(HOUR FROM data_received_timestamp_utc)::int % 3) * INTERVAL '1 hour') AS record_date,
    TRUNC(AVG(rw.temperature), 1) AS avg_temp,
    TRUNC(AVG(rw.humidity), 1) AS avg_humidity,
    TRUNC(AVG(rw.wind_speed), 2) AS avg_wind_speed,
    TRUNC(AVG(rw.visibility), 1) AS avg_visibility,
    MAX(rw.sunrise_utc) AS sunrise_utc,
    MAX(rw.sunset_utc) AS sunset_utc
    FROM staging.raw_weather_data rw
    WHERE rw.data_received_timestamp_utc::DATE = '{{ ds }}'::DATE
    GROUP BY rw.country, rw.city_name, record_date
),

majority_weather AS (
  SELECT DISTINCT ON (country, city_name, record_date)
    country,
    city_name,
    record_date,
    weather_description AS maj_weather_description
  FROM (
    SELECT
      rw.country,
      rw.city_name,
      DATE_TRUNC('hour', rw.data_received_timestamp_utc)
        - ((EXTRACT(HOUR FROM rw.data_received_timestamp_utc)::int % 3) * INTERVAL '1 hour') AS record_date,
      rw.weather_description,
      COUNT(*) OVER (
        PARTITION BY
          rw.country, rw.city_name, 
          DATE_TRUNC('hour', rw.data_received_timestamp_utc)
            - ((EXTRACT(HOUR FROM rw.data_received_timestamp_utc)::int % 3) * INTERVAL '1 hour'),
          rw.weather_description
      ) AS desc_count
    FROM staging.raw_weather_data rw
    WHERE rw.data_received_timestamp_utc::DATE = '{{ ds }}'::DATE
  ) sub
  ORDER BY country, city_name, record_date, desc_count DESC
)

INSERT INTO public.weather_facts_every_3h (
  country,
  city_name,
  record_date,
  avg_temp,
  avg_humidity,
  maj_weather_description,
  avg_wind_speed,
  avg_visibility,
  load_timestamp,
  is_dark
)
SELECT
  am.country,
  am.city_name,
  am.record_date,
  am.avg_temp,
  am.avg_humidity,
  mw.maj_weather_description,
  am.avg_wind_speed,
  am.avg_visibility,
  CURRENT_TIMESTAMP,
  CASE
    WHEN am.record_date >= (am.sunrise_utc - INTERVAL '60 minutes')
         AND am.record_date < am.sunrise_utc THEN 1 -- Dawn
    WHEN am.record_date >= am.sunset_utc
         AND am.record_date <= (am.sunset_utc + INTERVAL '80 minutes') THEN 2 -- Twilight
    WHEN am.record_date < (am.sunrise_utc - INTERVAL '60 minutes')
         OR am.record_date > (am.sunset_utc + INTERVAL '80 minutes') THEN 3 -- Dark
    ELSE 0 -- Daylight
  END AS is_dark
FROM avg_metrics am
JOIN majority_weather mw
  ON am.country = mw.country
  AND am.city_name = mw.city_name
  AND am.record_date = mw.record_date
ON CONFLICT (country, city_name, record_date) DO UPDATE SET
  avg_temp = EXCLUDED.avg_temp,
  avg_humidity = EXCLUDED.avg_humidity,
  maj_weather_description = EXCLUDED.maj_weather_description,
  avg_wind_speed = EXCLUDED.avg_wind_speed,
  avg_visibility = EXCLUDED.avg_visibility,
  is_dark = EXCLUDED.is_dark,
  load_timestamp = CURRENT_TIMESTAMP;