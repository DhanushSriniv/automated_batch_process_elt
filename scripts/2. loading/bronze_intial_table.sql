/* Bronze Initial Table Creation

    Purpose:
    - To create the initial tables in the bronze layer for the Bike-Share-Toronto database.
    - To store raw data from the bike-share system and GBFS specification.  
    - To maintain a historical record of station information, status, 
      system information, pricing plans, and other relevant data.

    Note:
        - bike-share-json : bst
        - gbfs-specification : gbfs
*/

-- 1. bike-share-json tables

CREATE Table bronze.bst_station_information_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    station_information JSON
);

CREATE Table bronze.bst_station_status_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    station_status JSON
);

CREATE Table bronze.bst_system_information_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    system_information JSON
);

CREATE Table bronze.bst_system_pricing_plans_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pricing_plans JSON
);

CREATE Table bronze.bst_system_regions_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    system_regions JSON
);

-- 2. gbfs-specification tables

CREATE Table bronze.gbfs_station_information_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    station_information JSON
);

CREATE Table bronze.gbfs_station_status_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    station_status JSON
);

CREATE Table bronze.gbfs_system_information_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    system_information JSON
);

CREATE Table bronze.gbfs_system_pricing_plans_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pricing_plans JSON
);

CREATE Table bronze.gbfs_system_regions_raw (
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    system_regions JSON
);

