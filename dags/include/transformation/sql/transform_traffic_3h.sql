CREATE TABLE IF NOT EXISTS public.traffic_facts_every_3h(
    city VARCHAR(255),
    frc VARCHAR(10),
    avg_speed NUMERIC,
    avg_freeflowspeed NUMERIC,
    avg_traveltime NUMERIC, --is in kmh
    avg_confidence NUMERIC,
    road_closure_occurances INTEGER,
    rough_latitude NUMERIC,
    rough_longitude NUMERIC,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, frc, rough_latitude, rough_longitude, load_timestamp)
);

INSERT INTO public.traffic_facts_every_3h (
    city,
    frc,
    avg_speed,
    avg_freeflowspeed,
    avg_traveltime,
    avg_confidence,
    road_closure_occurances,
    rough_latitude,
    rough_longitude,
    load_timestamp
)
SELECT
    rt.city,
    rt.frc,
    TRUNC(AVG(rt.currentSpeed),1) AS avg_speed,
    TRUNC(AVG(rt.freeFlowSpeed),1) AS avg_freeflowspeed,
    TRUNC(AVG(rt.currentTravelTime),1) AS avg_traveltime,
    AVG(rt.confidence) AS avg_confidence,
    SUM(rt.roadClosure::int) AS road_closure_occurances,
    TRUNC(AVG(rc.latitude),3) AS rough_latitude,
    TRUNC(AVG(rc.longitude),3) AS rough_longitude,
    DATE_TRUNC('second', CURRENT_TIMESTAMP) AS load_timestamp
FROM
    staging.raw_traffic_data rt
INNER JOIN    
    staging.raw_coord_data rc ON rt.id = rc.traffic_data_id
WHERE
    rt.extraction_timestamp_utc::DATE = '{{ ds }}'::DATE
GROUP BY
    rt.city,
    rt.frc,
    TRUNC(AVG(rc.latitude), 3),
    TRUNC(AVG(rc.longitude), 3),
    DATE_TRUNC('day', rt.extraction_timestamp_utc)
ON CONFLICT (city, frc, rough_latitude, rough_longitude, load_timestamp) DO UPDATE SET
    avg_speed = EXCLUDED.avg_speed,
    avg_freeflowspeed = EXCLUDED.avg_freeflowspeed,
    avg_traveltime = EXCLUDED.avg_traveltime,
    avg_confidence = EXCLUDED.avg_confidence,
    road_closure_occurances = EXCLUDED.road_closure_occurances;