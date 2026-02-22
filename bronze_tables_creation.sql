/*
    Bronze Layer Tables for Bike-Share-Toronto Database

    Purpose:
    - To store raw data from the bike-share system and GBFS specification.
    - To maintain a historical record of station information, status, 
      system information, pricing plans,

    Note:
        - bike-share-json : bst
        - gbfs-specification : gbfs
*/

-- Bike-Share-Toronto  

-- 1. station_information
CREATE TABLE bronze.bst_station_information (
    station_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    physical_configuration VARCHAR(100),
    lat NUMERIC(10,8),
    lon NUMERIC(11,8),
    address VARCHAR(255),
    capacity INTEGER,
    is_charging_station BOOLEAN,
    rental_methods TEXT[],
    groups TEXT[],
    obcn VARCHAR(100),
    short_name VARCHAR(100),
    nearby_distance NUMERIC(10,4),
    _ride_code_support BOOLEAN,
    rental_uris JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. station_status
CREATE TABLE bronze.bst_station_status (
    station_id VARCHAR(100) PRIMARY KEY,
    num_bikes_available INTEGER,
    num_bikes_disabled INTEGER,
    status VARCHAR(50),
    traffic JSONB,
    num_bikes_available_types JSONB,
    num_docks_available INTEGER,
    num_docks_disabled INTEGER,
    last_reported INTEGER,
    is_installed INTEGER,
    is_renting INTEGER,
    is_returning INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. System_Information
CREATE TABLE bronze.bst_system_information (
    system_id VARCHAR(100) PRIMARY KEY,
    timezone VARCHAR(100),
    build_version VARCHAR(100),
    build_label VARCHAR(100),
    build_hash VARCHAR(100),
    build_number VARCHAR(100),
    mobile_head_version VARCHAR(100),
    mobile_minimum_supported_version VARCHAR(100),
    _vehicle_count JSONB,
    _station_count INTEGER,
    language VARCHAR(50),
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Pricing Plans

CREATE TABLE bronze.bst_plans (
    plan_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    currency VARCHAR(10),
    price NUMERIC(10,2),
    description TEXT,
    is_taxable INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. System Regions
CREATE TABLE bronze.bst_system_regions (
    id SERIAL PRIMARY KEY,
    last_updated INTEGER,
    ttl INTEGER,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GBFS Specification

-- 1. station_information
CREATE TABLE bronze.gbfs_station_information (
    station_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    physical_configuration VARCHAR(100),
    lat NUMERIC(10,8),
    lon NUMERIC(11,8),
    address VARCHAR(255),
    capacity INTEGER,
    is_charging_station BOOLEAN,
    rental_methods TEXT[],
    groups TEXT[],
    obcn VARCHAR(100),
    short_name VARCHAR(100),
    nearby_distance NUMERIC(10,4),
    _ride_code_support BOOLEAN,
    rental_uris JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. station_status
CREATE TABLE bronze.gbfs_station_status (
    station_id VARCHAR(100) PRIMARY KEY,
    num_bikes_available INTEGER,
    num_bikes_disabled INTEGER,
    status VARCHAR(50),
    traffic JSONB,
    num_bikes_available_types JSONB,
    num_docks_available INTEGER,
    num_docks_disabled INTEGER,
    last_reported INTEGER,
    is_installed INTEGER,
    is_renting INTEGER,
    is_returning INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. System_Information
CREATE TABLE bronze.gbfs_system_information (
    system_id VARCHAR(100) PRIMARY KEY,
    timezone VARCHAR(100),
    build_version VARCHAR(100),
    build_label VARCHAR(100),
    build_hash VARCHAR(100),
    build_number VARCHAR(100),
    mobile_head_version VARCHAR(100),
    mobile_minimum_supported_version VARCHAR(100),
    _vehicle_count JSONB,
    _station_count INTEGER,
    language VARCHAR(50),
    name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Pricing Plans

CREATE TABLE bronze.gbfs_pricing_plans (
    plan_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    currency VARCHAR(10),
    price NUMERIC(10,2),
    description TEXT,
    is_taxable INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. System Regions
CREATE TABLE bronze.gbfs_system_regions (
    id SERIAL PRIMARY KEY,
    last_updated INTEGER,
    ttl INTEGER,
    data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);