"""
Detailed Endpoint Data Comparison

Purpose:
    Fetch data from actual endpoints and show detailed structural differences
    to understand what makes up the real bike share data.
"""

import os
import sys
import json
from datetime import datetime

# Dynamically find the project root
current_dir = os.path.dirname(os.path.abspath(__file__))
while not os.path.exists(os.path.join(current_dir, 'utils')):
    parent = os.path.dirname(current_dir)
    if parent == current_dir:
        raise RuntimeError("Could not find project root (utils folder not found)")
    current_dir = parent
sys.path.insert(0, current_dir)

from utils.el_global import load_json_from_file, fetch_json


def show_station_sample():
    """Show a sample of actual station data."""
    print("\n" + "="*80)
    print("🚉 SAMPLE: Station Information Data")
    print("="*80)
    
    url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"
    print(f"\nFetching from: {url}\n")
    
    try:
        data = fetch_json(url)
        stations = data.get("data", {}).get("stations", [])
        
        print(f"📊 Total stations: {len(stations)}\n")
        
        if stations:
            sample = stations[0]
            print("📍 Sample Station Data (First Station):")
            print(f"\n{json.dumps(sample, indent=2)}\n")
            
            # Analyze fields
            print("📋 Available Fields in Station Data:")
            for field in sorted(sample.keys()):
                value = sample[field]
                print(f"  • {field}: {type(value).__name__} = {value if not isinstance(value, (dict, list)) else f'[{type(value).__name__}]'}")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")


def show_station_status_sample():
    """Show a sample of station status data."""
    print("\n" + "="*80)
    print("📈 SAMPLE: Station Status Data (Real-time)")
    print("="*80)
    
    url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status"
    print(f"\nFetching from: {url}\n")
    
    try:
        data = fetch_json(url)
        stations = data.get("data", {}).get("stations", [])
        
        print(f"📊 Total stations with status: {len(stations)}\n")
        
        if stations:
            sample = stations[0]
            print("⚙️  Sample Station Status (First Station):")
            print(f"\n{json.dumps(sample, indent=2)}\n")
            
            # Analyze fields
            print("📋 Available Fields in Status Data:")
            for field in sorted(sample.keys()):
                value = sample[field]
                value_display = value if not isinstance(value, (dict, list)) else f'[{type(value).__name__}]'
                print(f"  • {field}: {type(value).__name__} = {value_display}")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")


def compare_data_types():
    """Compare the two file types."""
    print("\n" + "="*80)
    print("🔄 METADATA vs ACTUAL DATA COMPARISON")
    print("="*80)
    
    # Load metadata
    metadata_path = os.path.join(current_dir, "data/gbfs_feeds/feeds_data/bike-share-json/feeds_summary.json")
    metadata = load_json_from_file(metadata_path)
    
    comparison = f"""
    METADATA FILE (feeds_summary.json)
    ├─ Type: Discovery/Index
    ├─ Size: ~1-2 KB
    ├─ Purpose: Tell you WHERE to get data
    ├─ Content:
    │  └─ URLs to 5 different data endpoints
    ├─ Frequency: Updated ~every batch run
    └─ Example Structure:
       {{
         "timestamp": "2026-02-18T03:38:34.213433",
         "resource": "bike-share-json",
         "feeds": {{
           "station_information": {{
             "url": "https://tor.publicbikesystem.net/.../station_information",
             "has_data": true
           }}
         }}
       }}
    
    ACTUAL DATA (from endpoint URLs)
    ├─ Type: Real operational data
    ├─ Size: 300-500 KB per endpoint
    ├─ Purpose: Provide ACTUAL bike share information
    ├─ Content:
    │  ├─ station_information: 700+ stations with coordinates, capacity
    │  └─ station_status: Real-time bikes/docks available at each station
    ├─ Frequency: Updated every 5-30 seconds
    └─ Example Structure:
       {{
         "last_updated": 1771577103,
         "data": {{
           "stations": [
             {{
               "station_id": "123",
               "name": "Bloor St & Avenue Rd",
               "lat": 43.667,
               "lon": -79.395,
               "capacity": 23
             }},
             // ... 700+ more stations
           ]
         }}
       }}
    
    KEY DIFFERENCE:
    ───────────────
    Metadata tells you WHERE → Actual data gives you WHAT
    """
    
    print(comparison)


def show_extraction_workflow():
    """Show how extraction works."""
    print("\n" + "="*80)
    print("🔄 DATA EXTRACTION WORKFLOW")
    print("="*80)
    
    workflow = """
    STEP 1: RUN EXTRACTION JOB
    ├─ Triggered: On schedule (e.g., every hour)
    │
    └─ Action: Hit the METADATA endpoint
       https://tor.publicbikesystem.net/...
       
    STEP 2: GET METADATA RESPONSE
    ├─ Response includes list of available feeds/endpoints
    ├─ Each feed has a URL pointing to actual data
    │
    └─ Save this as: feeds_summary.json
       (Your data/gbfs_feeds/feeds_data/bike-share-json/ folder)
    
    STEP 3: LOOP THROUGH EACH ENDPOINT IN METADATA
    ├─ For each URL in feeds_summary.json:
    │
    ├─ Hit: https://tor.publicbikesystem.net/...station_information
    ├─ Get: 510 KB of data with 700+ stations
    ├─ Save: station_information.json
    │
    ├─ Hit: https://tor.publicbikesystem.net/...station_status
    ├─ Get: 337 KB of real-time availability
    ├─ Save: station_status.json
    │
    ├─ Hit: https://tor.publicbikesystem.net/...system_pricing_plans
    ├─ Get: 8.43 KB of pricing info
    ├─ Save: system_pricing_plans.json
    │
    └─ ... and so on for other endpoints
    
    STEP 4: STORE IN LAYERS
    ├─ BRONZE LAYER: Keep raw JSON files
    ├─ SILVER LAYER: Transform to tables/parquet
    └─ GOLD LAYER: Create analytics/dashboards
    
    WHY TWO METADATA FILES?
    ───────────────────────
    Your extraction appears to run this process TWICE:
    
    RUN 1 (bike-share-json) → Timestamp: 03:38:34.213433
    └─ Saved as: data/gbfs_feeds/feeds_data/bike-share-json/
    
    RUN 2 (gbfs-specification) → Timestamp: 03:38:34.365408 (0.15s later)
    └─ Saved as: data/gbfs_feeds/feeds_data/gbfs-specification/
    
    Both produce identical data because they hit the same source!
    This could be:
    • Part of validation testing
    • Redundancy for reliability
    • Comparing different parsing methods
    • Schema compliance testing
    """
    
    print(workflow)


def main():
    """Main function."""
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + " "*20 + "DETAILED DATA STRUCTURE ANALYSIS" + " "*28 + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
    
    try:
        show_extraction_workflow()
        show_station_sample()
        show_station_status_sample()
        compare_data_types()
        
        print("\n" + "="*80)
        print("🎯 CONCLUSION")
        print("="*80)
        
        conclusion = """
The two file sets (bike-share-json and gbfs-specification) are METADATA snapshots
from the SAME source, both pointing to THE SAME live endpoints.

ACTUAL live data is 300-500 KB and retrieved from those endpoint URLs during
each extraction run. The metadata file is just 1-2 KB and tells you WHERE to find
the real data.

Your data pipeline should:
1. Load metadata (feeds_summary.json)
2. Extract URLs from metadata
3. Fetch actual data from those URLs
4. Store everything in your layered architecture (BRONZE → SILVER → GOLD)
5. Create useful datasets for analysis

The dual extraction is likely part of a testing/validation workflow to ensure
data consistency and API reliability.
"""
        
        print(conclusion)
        
        print("\n" + "█"*80)
        print("█" + " "*20 + "ANALYSIS COMPLETE" + " "*42 + "█")
        print("█"*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
