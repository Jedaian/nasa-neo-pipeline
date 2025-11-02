CREATE MATERIALIZED VIEW `nasa-neo-pipeline.neo_data.neo_daily_summary_mv`
PARTITION BY observation_date
OPTIONS(
  enable_refresh = TRUE,
  refresh_interval_minutes = 30
)
AS
SELECT 
    -- Date dimension
    observation_date,
    
    -- Volume metrics
    COUNT(*) as total_observations,
    APPROX_COUNT_DISTINCT(neo_id) as unique_neos,
    
    -- Hazard metrics
    COUNTIF(is_potentially_hazardous = TRUE) as hazardous_count,
    COUNTIF(is_potentially_hazardous = FALSE) as non_hazardous_count,
    
    -- Sentry metrics
    COUNTIF(is_sentry_object = TRUE) as sentry_count,
    COUNTIF(is_sentry_object = FALSE) as non_sentry_count,
    
    -- Distance metrics (in km) - raw values, no ROUND
    MIN(miss_distance_km) as closest_approach_km,
    MAX(miss_distance_km) as farthest_approach_km,
    AVG(miss_distance_km) as avg_miss_distance_km,
    SUM(miss_distance_km) as total_miss_distance_km,
    
    -- Distance metrics (in lunar distances)
    MIN(miss_distance_lunar) as closest_approach_lunar,
    MAX(miss_distance_lunar) as farthest_approach_lunar,
    AVG(miss_distance_lunar) as avg_miss_distance_lunar,
    
    -- Size metrics (in meters)
    MIN(estimated_diameter_min_meters) as smallest_neo_min_m,
    MAX(estimated_diameter_max_meters) as largest_neo_max_m,
    AVG(estimated_diameter_max_meters) as avg_diameter_m,
    AVG(estimated_diameter_min_meters) as avg_diameter_min_m,
    
    -- Velocity metrics (in km/h)
    MIN(relative_velocity_kmh) as slowest_velocity_kmh,
    MAX(relative_velocity_kmh) as fastest_velocity_kmh,
    AVG(relative_velocity_kmh) as avg_velocity_kmh,
    
    -- Orbiting body counts
    COUNTIF(orbiting_body = 'Earth') as orbiting_earth_count,
    COUNTIF(orbiting_body = 'Mars') as orbiting_mars_count
    
FROM `nasa-neo-pipeline.neo_data.neo_observations`
GROUP BY observation_date;