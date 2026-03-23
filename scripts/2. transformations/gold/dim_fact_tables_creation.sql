

-- 1. gold.dim_station — SCD Type 2
CREATE TABLE gold.dim_station (
    station_key          SERIAL PRIMARY KEY,        -- surrogate key
    station_id           VARCHAR(10)  NOT NULL,     -- business key
    station_name         VARCHAR(255),
    address              VARCHAR(255),
    lat                  DECIMAL(10,8),
    lon                  DECIMAL(11,8),
    capacity             INT,
    physical_configuration VARCHAR(50),             -- REGULAR/ELECTRICBIKESTATION/VAULT
    is_charging_station  BOOLEAN,
    nearby_distance      DECIMAL(10,2),
    ride_code_support    BOOLEAN,
    -- SCD Type 2
    valid_from           TIMESTAMP    NOT NULL,
    valid_to             TIMESTAMP    DEFAULT '9999-12-31',
    is_current           BOOLEAN      DEFAULT TRUE,
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);


-- 2. gold.dim_geography — Type 1
CREATE TABLE gold.dim_geography (
    geography_key        SERIAL PRIMARY KEY,
    station_id           VARCHAR(10),               -- link back to station
    region               VARCHAR(100),              -- "South", "East", "North"
    neighborhood         VARCHAR(150),              -- "Financial District"
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- 3. gold.dim_time — Type 1
CREATE TABLE gold.dim_time (
    time_key             SERIAL PRIMARY KEY,
    snapshot_timestamp   TIMESTAMP    NOT NULL,     -- converted from Unix epoch
    snapshot_date        DATE,
    hour_of_day          INT,                       -- 0-23
    day_of_week          INT,                       -- 1=Mon, 7=Sun
    day_name             VARCHAR(10),
    is_weekend           BOOLEAN,
    is_peak_hour         BOOLEAN,                   -- 7-9AM, 4-7PM
    is_morning_peak      BOOLEAN,
    is_evening_peak      BOOLEAN,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. gold.dim_pricing_plan — Type 1
CREATE TABLE gold.dim_pricing_plan (
    plan_key             SERIAL PRIMARY KEY,
    plan_id              INT,
    plan_name            VARCHAR(100),              -- Annual 30, Corporate 45
    currency             VARCHAR(3),               -- CAD
    price                DECIMAL(10,2),
    description          TEXT,
    is_taxable           BOOLEAN,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. gold.fact_station_availability — Fact Table (Grain: 1 row per station per 12hr snapshot)
CREATE TABLE gold.fact_station_availability (
    fact_key BIGSERIAL PRIMARY KEY,

    -- FK dimensions (enforced)
    station_key     INT NOT NULL REFERENCES gold.dim_station(station_key),
    geography_key   INT REFERENCES gold.dim_geography(geography_key),
    time_key        INT NOT NULL REFERENCES gold.dim_time(time_key),

    -- Raw measures
    num_bikes_available         INT NOT NULL,
    num_bikes_disabled          INT,
    num_docks_available         INT NOT NULL,
    num_docks_disabled          INT,
    ebikes_available            INT DEFAULT 0,
    mechanical_bikes_available  INT DEFAULT 0,

    -- Status flags
    status          VARCHAR(20),
    is_installed    BOOLEAN NOT NULL,
    is_renting      BOOLEAN NOT NULL,
    is_returning    BOOLEAN NOT NULL,

    -- Derived metrics
    capacity            INT NOT NULL,
    availability_rate   DECIMAL(5,4),
    utilization_pct     DECIMAL(5,4),
    rebalancing_needed  BOOLEAN,
    rebalance_action    VARCHAR(20),

    -- Time tracking
    snapshot_timestamp  TIMESTAMP NOT NULL,
    load_dts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 🚀 Enforce grain
    CONSTRAINT uq_station_snapshot UNIQUE (station_key, snapshot_timestamp)
);

-- Fast joins
CREATE INDEX idx_fact_station_key ON gold.fact_station_availability(station_key);
CREATE INDEX idx_fact_time_key ON gold.fact_station_availability(time_key);

-- Time-based filtering (most common query)
CREATE INDEX idx_fact_snapshot_time ON gold.fact_station_availability(snapshot_timestamp);

-- Composite (very powerful for analytics)
CREATE INDEX idx_fact_station_time 
ON gold.fact_station_availability(station_key, snapshot_timestamp);