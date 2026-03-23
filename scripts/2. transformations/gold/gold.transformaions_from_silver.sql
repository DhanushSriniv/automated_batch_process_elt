/*
    Title: Gold Transformations from Silver

    Description:
    - This SQL script performs the transformations from the Silver layer to populate the Gold layer of our data warehouse.
    - It includes the following transformations:
        * dim_pricing_plan: Transforms pricing plan data from silver.bst_plans to gold.dim_pricing_plan.
        * dim_geography: Extracts region and neighborhood information from silver.bst_station_information and populates gold.dim_geography.
        * dim_station: Implements Type 2 Slowly Changing Dimension logic to track changes in station attributes over time in gold.dim_station.
        * dim_time: Converts Unix epoch timestamps from silver.bst_station_status into rich time attributes in gold.dim_time.
        * fact_station_availability: Combines data from dimensions and silver.bst_station_status to populate the fact table gold.fact_station_availability with measures and status flags.
    - The script uses incremental loading techniques and conflict handling to ensure data integrity and efficient processing.

    Usage:
    - Run this script after the silver layer has been populated and the dimension tables in the gold layer have been created.
    - Ensure that the necessary indexes and constraints are in place on the gold tables for optimal performance.
*/



-- 1. dim_pricing_plan

INSERT INTO gold.dim_pricing_plan
    (plan_id, plan_name, currency, price, description, is_taxable)
SELECT DISTINCT
    plan_id::INT,
    name,
    currency,
    price::DECIMAL(10,2),
    description,
    is_taxable::BOOLEAN
FROM silver.bst_plans
WHERE plan_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- 2. dim_geography
INSERT INTO gold.dim_geography (
    station_id,
    region,
    neighborhood
)
SELECT
    station_id,

    -- Region detection
    MAX(
        CASE 
            WHEN LOWER(TRIM(elem)) IN ('south', 'east', 'west', 'north') 
            THEN UPPER(SUBSTR(TRIM(elem), 1, 1)) || LOWER(SUBSTR(TRIM(elem), 2))
        END
    ) AS region,

    -- Neighborhood (anything not classified)
    MAX(
        CASE 
            WHEN LOWER(TRIM(elem)) NOT IN (
                'south','east','west','north',
                'e-charging','valet stations','e-bike'
            )
            THEN UPPER(SUBSTR(TRIM(elem), 1, 1)) || LOWER(SUBSTR(TRIM(elem), 2))
        END
    ) AS neighborhood

FROM silver.bst_station_information
CROSS JOIN LATERAL UNNEST(groups) AS elem
WHERE groups IS NOT NULL
GROUP BY station_id
ON CONFLICT DO NOTHING;


-- 3. dim_station
-- Expire changed records
UPDATE gold.dim_station ds
SET
    valid_to   = si.updated_at,
    is_current = FALSE
FROM silver.bst_station_information si
WHERE ds.station_id = si.station_id
  AND ds.is_current = TRUE
  AND (
    ds.capacity IS DISTINCT FROM si.capacity 
    OR ds.physical_configuration IS DISTINCT FROM si.physical_configuration
    )
  AND si.updated_at > ds.valid_from;


-- Insert new / changed stations
INSERT INTO gold.dim_station
    (station_id, station_name, address, lat, lon, capacity,
     physical_configuration, is_charging_station,
     nearby_distance, ride_code_support,
     valid_from, valid_to, is_current)
SELECT
    si.station_id, si.name, si.address, si.lat, si.lon, si.capacity,
    si.physical_configuration, si.is_charging_station,
    si.nearby_distance, si._ride_code_support as ride_code_support,
    si.updated_at, '9999-12-31'::TIMESTAMP, TRUE
FROM silver.bst_station_information si
WHERE NOT EXISTS (
    SELECT 1 
    FROM gold.dim_station ds
    WHERE ds.station_id           = si.station_id
      AND ds.is_current           = TRUE
      AND ds.capacity IS NOT DISTINCT FROM si.capacity
      AND ds.physical_configuration IS NOT DISTINCT FROM si.physical_configuration
);


-- 4. dim_time
INSERT INTO gold.dim_time
    (snapshot_timestamp, snapshot_date, hour_of_day, day_of_week,
     day_name, is_weekend, is_peak_hour, is_morning_peak, is_evening_peak)
SELECT DISTINCT
    ts AS snapshot_timestamp,
    ts::DATE AS snapshot_date,
    EXTRACT(HOUR FROM ts)::INT AS hour_of_day,
    EXTRACT(ISODOW FROM ts)::INT AS day_of_week,
    TRIM(TO_CHAR(ts, 'Day')) AS day_name,
    EXTRACT(ISODOW FROM ts) IN (6,7) AS is_weekend,
    EXTRACT(HOUR FROM ts) IN (7,8,9,16,17,18,19) AS is_peak_hour,
    EXTRACT(HOUR FROM ts) IN (7,8,9) AS is_morning_peak,
    EXTRACT(HOUR FROM ts) IN (16,17,18,19) AS is_evening_peak
FROM (
    SELECT 
        TO_TIMESTAMP(ss.last_reported) AS ts
    FROM silver.bst_station_status ss
) t
WHERE NOT EXISTS (
    SELECT 1 
    FROM gold.dim_time dt
    WHERE dt.snapshot_timestamp = t.ts
);

-- 5. fact_station_availability
INSERT INTO gold.fact_station_availability (
    station_key, geography_key, time_key,
    num_bikes_available, num_bikes_disabled,
    num_docks_available, num_docks_disabled,
    ebikes_available, mechanical_bikes_available,
    status, is_installed, is_renting, is_returning,
    capacity, availability_rate, utilization_pct,
    rebalancing_needed, rebalance_action, snapshot_timestamp
)
SELECT
    ds.station_key,
    dg.geography_key,
    dt.time_key,

    -- Raw
    s.num_bikes_available,
    s.num_bikes_disabled,
    s.num_docks_available,
    s.num_docks_disabled,

    -- JSON parsed (safe)
    COALESCE((s.num_bikes_available_types->>'ebikecount')::INT, 0),
    COALESCE((s.num_bikes_available_types->>'mechanical_count')::INT, 0),

    -- Status
    s.status,
    (s.is_installed = 1),
    (s.is_renting = 1),
    (s.is_returning = 1),

    -- Derived
    ds.capacity,

    ROUND(s.num_bikes_available::DECIMAL / NULLIF(ds.capacity, 0), 4),
    ROUND((ds.capacity - s.num_docks_available)::DECIMAL / NULLIF(ds.capacity, 0), 4),

    -- Rebalancing
    CASE
        WHEN s.num_bikes_available::DECIMAL / NULLIF(ds.capacity,0) < 0.2
          OR s.num_docks_available::DECIMAL / NULLIF(ds.capacity,0) < 0.2
        THEN TRUE ELSE FALSE
    END,

    CASE
        WHEN s.num_bikes_available = 0 THEN 'CRITICAL_EMPTY'
        WHEN s.num_docks_available = 0 THEN 'CRITICAL_FULL'
        WHEN s.num_bikes_available::DECIMAL / NULLIF(ds.capacity,0) < 0.2 THEN 'ADD_BIKES'
        WHEN s.num_docks_available::DECIMAL / NULLIF(ds.capacity,0) < 0.2 THEN 'REMOVE_BIKES'
        ELSE 'OK'
    END,

    s.ts

FROM (
    -- Compute timestamp once
    SELECT 
        ss.*,
        TO_TIMESTAMP(ss.last_reported) AS ts
    FROM silver.bst_station_status ss
) s

-- Dimensions
JOIN gold.dim_station ds 
  ON ds.station_id = s.station_id 
 AND ds.is_current = TRUE

JOIN gold.dim_time dt 
  ON dt.snapshot_timestamp = s.ts

LEFT JOIN gold.dim_geography dg 
  ON dg.station_id = s.station_id

-- Incremental load (optional but efficient)
WHERE s.ts > (
    SELECT COALESCE(MAX(snapshot_timestamp), '1900-01-01')
    FROM gold.fact_station_availability
)

-- 🚀 Prevent duplicates (critical)
ON CONFLICT (station_key, snapshot_timestamp) DO NOTHING;