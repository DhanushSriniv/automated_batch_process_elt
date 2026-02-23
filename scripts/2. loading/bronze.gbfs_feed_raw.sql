
-- This table is designed to store raw GBFS feed data as ingested, without any transformation.
-- It serves as a landing zone for all GBFS feed data, allowing us to maintain a complete historical 
-- record of the raw data as it was received from the source.
DROP TABLE IF EXISTS bronze.gbfs_feed_raw;

CREATE TABLE IF NOT EXISTS bronze.gbfs_feed_raw (
    id              bigserial PRIMARY KEY,
    feed_type       text        NOT NULL,   -- e.g. 'station_information'
    source_name     text        NOT NULL,   -- 'bike-share-json' or 'bike-share-gbfs-general-bikeshare-feed-specification'
    load_batch_id   text        NOT NULL,   -- e.g. run timestamp or UUID
    file_name       text        NOT NULL,   -- logical name: 'station_information.json'
    api_url         text        NOT NULL,   -- feed URL
    version         text,                  -- GBFS version if present
    time_ingested   timestamptz NOT NULL DEFAULT now(),
    raw_payload     jsonb       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gbfs_feed_raw_type_time
    ON bronze.gbfs_feed_raw (feed_type, time_ingested);

CREATE INDEX IF NOT EXISTS idx_gbfs_feed_raw_gin
    ON bronze.gbfs_feed_raw USING gin (raw_payload);
