"""Load transformed data into the SILVER layer.

This script reads raw JSON payloads stored in `bronze.gbfs_feed_raw` (source_name = 'bike-share-json')
and extracts the GBFS feed objects into normalized SILVER tables.

Usage:
    python scripts/3. transformations/silver/load_silver.py
"""


import os
import sys
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
while not os.path.exists(os.path.join(current_dir, 'utils')):
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        raise RuntimeError("Could not find project root (utils folder not found)")
    current_dir = parent
sys.path.insert(0, current_dir)

from utils.db import get_pg_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def load_station_information(cur):
    logger.info("Loading silver.bst_station_information from bronze.gbfs_feed_raw")
    cur.execute(
        """
        WITH station_rows AS (
            SELECT
                (jsonb_array_elements(raw_payload->'data'->'stations')) AS station,
                time_ingested
            FROM bronze.gbfs_feed_raw
            WHERE source_name = 'bike-share-json'
              AND feed_type = 'station_information'
        ),
        latest AS (
            SELECT DISTINCT ON (station->>'station_id') station
            FROM station_rows
            ORDER BY station->>'station_id', time_ingested DESC
        )
        INSERT INTO silver.bst_station_information (
            station_id,
            name,
            physical_configuration,
            lat,
            lon,
            address,
            capacity,
            is_charging_station,
            rental_methods,
            groups,
            obcn,
            short_name,
            nearby_distance,
            _ride_code_support,
            rental_uris
        )
        SELECT
            station->>'station_id',
            station->>'name',
            station->>'physical_configuration',
            NULLIF(station->>'lat', '')::numeric,
            NULLIF(station->>'lon', '')::numeric,
            station->>'address',
            NULLIF(station->>'capacity', '')::int,
            NULLIF(station->>'is_charging_station', '')::boolean,
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(station->'rental_methods', '[]'::jsonb))),
            ARRAY(SELECT jsonb_array_elements_text(COALESCE(station->'groups', '[]'::jsonb))),
            station->>'obcn',
            station->>'short_name',
            NULLIF(station->>'nearby_distance', '')::numeric,
            NULLIF(station->>'_ride_code_support', '')::boolean,
            station->'rental_uris'
        FROM latest
        ON CONFLICT (station_id) DO UPDATE SET
            name = EXCLUDED.name,
            physical_configuration = EXCLUDED.physical_configuration,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            address = EXCLUDED.address,
            capacity = EXCLUDED.capacity,
            is_charging_station = EXCLUDED.is_charging_station,
            rental_methods = EXCLUDED.rental_methods,
            groups = EXCLUDED.groups,
            obcn = EXCLUDED.obcn,
            short_name = EXCLUDED.short_name,
            nearby_distance = EXCLUDED.nearby_distance,
            _ride_code_support = EXCLUDED._ride_code_support,
            rental_uris = EXCLUDED.rental_uris,
            updated_at = now();
        """
    )


def load_station_status(cur):
    logger.info("Loading silver.bst_station_status from bronze.gbfs_feed_raw")
    cur.execute(
        """
        WITH status_rows AS (
            SELECT
                (jsonb_array_elements(raw_payload->'data'->'stations')) AS station,
                time_ingested
            FROM bronze.gbfs_feed_raw
            WHERE source_name = 'bike-share-json'
              AND feed_type = 'station_status'
        ),
        latest AS (
            SELECT DISTINCT ON (station->>'station_id') station
            FROM status_rows
            ORDER BY station->>'station_id', time_ingested DESC
        )
        INSERT INTO silver.bst_station_status (
            station_id,
            num_bikes_available,
            num_bikes_disabled,
            status,
            traffic,
            num_bikes_available_types,
            num_docks_available,
            num_docks_disabled,
            last_reported,
            is_installed,
            is_renting,
            is_returning
        )
        SELECT
            station->>'station_id',
            NULLIF(station->>'num_bikes_available', '')::int,
            NULLIF(station->>'num_bikes_disabled', '')::int,
            station->>'status',
            station->'traffic',
            station->'num_bikes_available_types',
            NULLIF(station->>'num_docks_available', '')::int,
            NULLIF(station->>'num_docks_disabled', '')::int,
            NULLIF(station->>'last_reported', '')::int,
            NULLIF(station->>'is_installed', '')::int,
            NULLIF(station->>'is_renting', '')::int,
            NULLIF(station->>'is_returning', '')::int
        FROM latest
        ON CONFLICT (station_id) DO UPDATE SET
            num_bikes_available = EXCLUDED.num_bikes_available,
            num_bikes_disabled = EXCLUDED.num_bikes_disabled,
            status = EXCLUDED.status,
            traffic = EXCLUDED.traffic,
            num_bikes_available_types = EXCLUDED.num_bikes_available_types,
            num_docks_available = EXCLUDED.num_docks_available,
            num_docks_disabled = EXCLUDED.num_docks_disabled,
            last_reported = EXCLUDED.last_reported,
            is_installed = EXCLUDED.is_installed,
            is_renting = EXCLUDED.is_renting,
            is_returning = EXCLUDED.is_returning,
            updated_at = now();
        """
    )


def load_system_information(cur):
    logger.info("Loading silver.bst_system_information from bronze.gbfs_feed_raw")
    cur.execute(
        """
        WITH info_rows AS (
            SELECT
                raw_payload->'data' AS info,
                time_ingested
            FROM bronze.gbfs_feed_raw
            WHERE source_name = 'bike-share-json'
              AND feed_type = 'system_information'
        ),
        latest AS (
            SELECT DISTINCT ON (info->>'system_id') info
            FROM info_rows
            ORDER BY info->>'system_id', time_ingested DESC
        )
        INSERT INTO silver.bst_system_information (
            system_id,
            timezone,
            build_version,
            build_label,
            build_hash,
            build_number,
            mobile_head_version,
            mobile_minimum_supported_version,
            _vehicle_count,
            _station_count,
            language,
            name
        )
        SELECT
            info->>'system_id',
            info->>'timezone',
            info->>'build_version',
            info->>'build_label',
            info->>'build_hash',
            info->>'build_number',
            info->>'mobile_head_version',
            info->>'mobile_minimum_supported_version',
            COALESCE(info->'vehicle_count', info->'_vehicle_count'),
            NULLIF(COALESCE(info->>'station_count', info->>'_station_count'), '')::int,
            info->>'language',
            info->>'name'
        FROM latest
        ON CONFLICT (system_id) DO UPDATE SET
            timezone = EXCLUDED.timezone,
            build_version = EXCLUDED.build_version,
            build_label = EXCLUDED.build_label,
            build_hash = EXCLUDED.build_hash,
            build_number = EXCLUDED.build_number,
            mobile_head_version = EXCLUDED.mobile_head_version,
            mobile_minimum_supported_version = EXCLUDED.mobile_minimum_supported_version,
            _vehicle_count = EXCLUDED._vehicle_count,
            _station_count = EXCLUDED._station_count,
            language = EXCLUDED.language,
            name = EXCLUDED.name,
            updated_at = now();
        """
    )


def load_plans(cur):
    logger.info("Loading silver.bst_plans from bronze.gbfs_feed_raw")
    cur.execute(
        """
        WITH plan_rows AS (
            SELECT
                jsonb_array_elements(raw_payload->'data'->'plans') AS plan,
                time_ingested
            FROM bronze.gbfs_feed_raw
            WHERE source_name = 'bike-share-json'
              AND feed_type = 'system_pricing_plans'
        ),
        latest AS (
            SELECT DISTINCT ON (plan->>'plan_id') plan
            FROM plan_rows
            ORDER BY plan->>'plan_id', time_ingested DESC
        )
        INSERT INTO silver.bst_plans (
            plan_id,
            name,
            currency,
            price,
            description,
            is_taxable
        )
        SELECT
            plan->>'plan_id',
            plan->>'name',
            plan->>'currency',
            NULLIF(plan->>'price', '')::numeric,
            plan->>'description',
            (NULLIF(plan->>'is_taxable', '')::boolean)::int
        FROM latest
        ON CONFLICT (plan_id) DO UPDATE SET
            name = EXCLUDED.name,
            currency = EXCLUDED.currency,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            is_taxable = EXCLUDED.is_taxable,
            updated_at = now();
        """
    )


def load_system_regions(cur):
    logger.info("Loading silver.bst_system_regions from bronze.gbfs_feed_raw")
    cur.execute(
        """
        INSERT INTO silver.bst_system_regions (last_updated, ttl, data)
        SELECT
            NULLIF(raw_payload->>'last_updated', '')::int,
            NULLIF(raw_payload->>'ttl', '')::int,
            raw_payload->'data'
        FROM bronze.gbfs_feed_raw
        WHERE source_name = 'bike-share-json'
          AND feed_type = 'system_regions'
        ORDER BY time_ingested DESC
        LIMIT 1;
        """
    )


def main():
    logger.info("Starting SILVER layer load")
    with get_pg_connection() as conn, conn.cursor() as cur:
        load_station_information(cur)
        load_station_status(cur)
        load_system_information(cur)
        load_plans(cur)
        load_system_regions(cur)
        conn.commit()
    logger.info("SILVER layer load complete")


if __name__ == "__main__":
    main()
