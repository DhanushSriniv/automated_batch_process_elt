import os
import json
import psycopg2


bst_json_file_paths = [
    'data/gbfs_feeds/feeds_data/bike-share-json/station_information.json',
    'data/gbfs_feeds/feeds_data/bike-share-json/station_status.json',
    'data/gbfs_feeds/feeds_data/bike-share-json/system_information.json',
    'data/gbfs_feeds/feeds_data/bike-share-json/system_pricing_plans.json', 
    'data/gbfs_feeds/feeds_data/bike-share-json/system_regions.json'
]

gbfs_json_file_paths = [
    'data/gbfs_feeds/feeds_data/gbfs-specification/station_information.json',
    'data/gbfs_feeds/feeds_data/gbfs-specification/station_status.json',
    'data/gbfs_feeds/feeds_data/gbfs-specification/system_information.json',
    'data/gbfs_feeds/feeds_data/gbfs-specification/system_pricing_plans.json',
    'data/gbfs_feeds/feeds_data/gbfs-specification/system_regions.json'
]

def load_json_from_file(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

bst_json_data_by_name = {os.path.splitext(os.path.basename(p))[0]: load_json_from_file(p) for p in bst_json_file_paths }
gbfs_json_data_by_name = {os.path.splitext(os.path.basename(p))[0]: load_json_from_file(p) for p in gbfs_json_file_paths }

# Connect to the database and create a cursor
conn = psycopg2.connect(dbname="toronto_bike_share_project",
                        port = 5433,
                        user="postgres",
                        password="Admin",
                        host="localhost")
cur = conn.cursor()

# Insert the JSON data into the corresponding tables for the bike-share-json dataset
cur.execute("TRUNCATE TABLE bronze.bst_station_information_raw;")           # Station information data is updated infrequently, so we can truncate the table before inserting the new data. This ensures that we only have the latest station information in the table and prevents the accumulation of outdated data over time.
cur.execute("""
    INSERT INTO bronze.bst_station_information_raw (station_information)
    VALUES (%s::jsonb)
""", (json.dumps(bst_json_data_by_name['station_information']),))

cur.execute("TRUNCATE TABLE bronze.bst_station_status_raw;")  # Station Status
cur.execute("""
    INSERT INTO bronze.bst_station_status_raw (station_status)
    VALUES (%s::jsonb)
""", (json.dumps(bst_json_data_by_name['station_status']),))

cur.execute("TRUNCATE TABLE bronze.bst_system_information_raw;")  # System Information
cur.execute("""
    INSERT INTO bronze.bst_system_information_raw (system_information)
    VALUES (%s::jsonb)
""", (json.dumps(bst_json_data_by_name['system_information']),))

cur.execute("TRUNCATE TABLE bronze.bst_system_pricing_plans_raw;") # System Pricing Plans
cur.execute("""
    INSERT INTO bronze.bst_system_pricing_plans_raw (pricing_plans)
    VALUES (%s::jsonb)
""", (json.dumps(bst_json_data_by_name['system_pricing_plans']),))

cur.execute("TRUNCATE TABLE bronze.bst_system_regions_raw;")      # System Regions
cur.execute("""
    INSERT INTO bronze.bst_system_regions_raw (system_regions)
    VALUES (%s::jsonb)
""", (json.dumps(bst_json_data_by_name['system_regions']),))

# Insert the JSON data into the corresponding tables for the gbfs-specification dataset
cur.execute("TRUNCATE TABLE bronze.gbfs_station_information_raw;")           # Station information data is updated infrequently, so we can truncate the table before inserting the new data. This ensures that we only have the latest station information in the table and prevents the accumulation of outdated data over time.
cur.execute("""
    INSERT INTO bronze.gbfs_station_information_raw (station_information)
    VALUES (%s::jsonb)
""", (json.dumps(gbfs_json_data_by_name['station_information']),))  

cur.execute("TRUNCATE TABLE bronze.gbfs_station_status_raw;")        # Station Status
cur.execute("""
    INSERT INTO bronze.gbfs_station_status_raw (station_status) 
    VALUES (%s::jsonb)
""", (json.dumps(gbfs_json_data_by_name['station_status']),))

cur.execute("TRUNCATE TABLE bronze.gbfs_system_information_raw;")   # System Information
cur.execute("""
    INSERT INTO bronze.gbfs_system_information_raw (system_information) 
    VALUES (%s::jsonb)
""", (json.dumps(gbfs_json_data_by_name['system_information']),))

cur.execute("TRUNCATE TABLE bronze.gbfs_system_pricing_plans_raw;")   # System Pricing Plans
cur.execute("""
    INSERT INTO bronze.gbfs_system_pricing_plans_raw (pricing_plans)
    VALUES (%s::jsonb)
""", (json.dumps(gbfs_json_data_by_name['system_pricing_plans']),))

cur.execute("TRUNCATE TABLE bronze.gbfs_system_regions_raw;")         # System Regions
cur.execute("""
    INSERT INTO bronze.gbfs_system_regions_raw (system_regions)
    VALUES (%s::jsonb)
""", (json.dumps(gbfs_json_data_by_name['system_regions']),))

# Commit the transaction and close the connection
conn.commit()   
cur.close()
conn.close()
