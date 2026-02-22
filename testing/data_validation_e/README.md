# Testing Scripts Guide

## Overview

This folder contains comprehensive testing and analysis scripts to help you understand your bike share data extraction pipeline and the two data source files.

---

## Quick Start

Run these scripts from the project root:

```bash
# Understanding the data sources
python testing/data_source_analysis.py

# Fetching and analyzing live endpoint data
python testing/live_data_endpoint_analysis.py

# Detailed data structure breakdown
python testing/detailed_data_structure.py

# Original comparison test
python testing/extraction_data_testing.py
```

---

## Scripts Overview

### 1. **data_source_analysis.py** 
**Purpose:** Understand why you have two metadata files and what makes them different.

**What it does:**
- Loads both `bike-share-json` and `gbfs-specification` metadata files
- Compares their structure, timestamps, and endpoints
- Shows all the endpoints they contain
- Highlights differences (or lack thereof)

**Key insights:**
- ✓ All 5 endpoints are identical between both files
- ✓ Extracted within 0.15 seconds of each other
- ✓ Both have the same 5 feeds available
- ✓ Only differences: timestamp and resource name

**Run time:** ~1 second
**Output:** Formatted tables showing metadata comparison

---

### 2. **live_data_endpoint_analysis.py**
**Purpose:** Fetch actual live data from the endpoints to understand scale and structure.

**What it does:**
- Loads endpoint URLs from metadata
- Attempts to fetch live data from each endpoint
- Measures response times
- Shows data sizes and key information
- Provides architecture recommendations

**Key findings:**
- ✓ All 5 endpoints are responsive
- ✓ `station_information`: 510 KB (1,008 stations)
- ✓ `station_status`: 337 KB (real-time availability)
- ✓ Endpoints provide live, real-time data
- ✓ Data is being actively maintained

**Run time:** ~5-10 seconds (network dependent)
**Output:** Endpoint status, response times, data sizes

---

### 3. **detailed_data_structure.py**
**Purpose:** See actual sample data to understand the data schema.

**What it does:**
- Fetches sample station data from live endpoint
- Fetches sample status data from live endpoint
- Shows the exact structure of real bike share data
- Explains the extraction workflow
- Clarifies metadata vs actual data difference

**Sample data includes:**
```json
{
  "station_id": "7000",
  "name": "Fort York  Blvd / Capreol Ct",
  "lat": 43.639832,
  "lon": -79.395954,
  "capacity": 47,
  "is_charging_station": false,
  "rental_methods": ["key", "transitcard", "creditcard"],
  "groups": ["South", "Fort York - Entertainment District"]
}
```

**Run time:** ~3-5 seconds (network dependent)
**Output:** Sample data structures with field explanations

---

### 4. **extraction_data_testing.py**
**Purpose:** Original testing script - compares the two metadata files for differences.

**What it does:**
- Loads both metadata files
- Uses `compare_json()` function from `el_global.py`
- Reports all differences found

**Expected output:**
```
Differences Found:
{'path': 'resource', 'value1': 'bike-share-json', 'value2': 'gbfs-specification'}
{'path': 'timestamp', 'value1': '2026-02-18T03:38:34.213433', ...}
```

**Run time:** ~1 second
**Output:** List of JSON differences

---

## Understanding Your Data Flow

```
┌─────────────────────────────────────┐
│   LIVE TORONTO BIKE SHARE API       │
│  (tor.publicbikesystem.net)         │
│   Updates every 5-30 seconds        │
└──────────────┬──────────────────────┘
               │
               │ EXTRACTION JOB runs
               │
         ┌─────▼─────┐
         │  TWO RUNS  │  (0.15s apart)
         └─────┬─────┘
               │
         ┌─────┴─────────────────┐
         │                       │
    ┌────▼────┐          ┌──────▼───┐
    │ RUN 1:  │          │  RUN 2:  │
    │ bike-   │          │  gbfs-   │
    │ share   │          │  spec    │
    │ -json   │          │          │
    └────┬────┘          └──────┬───┘
         │                      │
    Saves:                  Saves:
    feeds_summary.json      feeds_summary.json
    system_info.json        system_info.json
    ... etc                 ... etc
    
    Same endpoints! Same data!
    Different metadata names for
    testing/validation purposes
```

---

## Data Sizes Summary

| Component | Size | Count | Frequency |
|-----------|------|-------|-----------|
| Metadata (feeds_summary.json) | 1-2 KB | 1008 entries index | Per batch |
| station_information | 510 KB | 1,008 stations | Daily |
| station_status | 337 KB | 1,008 real-time | Every 5-30s |
| system_pricing_plans | 8.43 KB | 3-5 plans | As needed |
| system_information | 0.42 KB | 1 entry | Daily |
| system_regions | 0.06 KB | 1-3 regions | Rarely |
| **Total per extraction** | **~856 KB** | - | Every batch |

---

## Key Questions Answered

### Q: Which data is live?
**A:** All of it! The metadata files tell you WHERE, and the endpoint URLs point to live data.

### Q: Why two metadata files?
**A:** Likely testing/validation purposes. They contain identical information.

### Q: How often is data updated?
**A:** 
- Metadata files: Updated on each batch run (timestamps show this)
- Station status: Every 5-30 seconds (real-time)
- Station info: Daily or as needed

### Q: Can I use just one metadata file?
**A:** Yes! They're identical. Pick `bike-share-json` and ignore `gbfs-specification`.

### Q: Where's the REAL data?
**A:** In the data returned from the endpoint URLs inside the metadata files, particularly:
- `station_information` (locations, capacity)
- `station_status` (real-time availability)

---

## Module: utils/el_global.py

All scripts import from this global module that includes:

- `load_json_from_file(file_path)` - Load JSON from disk
- `fetch_json(url)` - Fetch JSON from URL
- `compare_json(json1, json2)` - Deep compare two JSON objects
- `get_package_metadata()` - Fetch CKAN metadata
- `extract_feeds()` - Parse feed structures
- Plus other utility functions

**How to add new functions:**
1. Add function to `utils/el_global.py`
2. Import in any script: `from utils.el_global import your_function`
3. Works from any subfolder automatically!

---

## Running Scripts from Windows PowerShell

```powershell
# Activate virtual environment
& "venv/Scripts/Activate.ps1"

# Run any script
python testing/data_source_analysis.py

# Or from project root with full path
python testing/live_data_endpoint_analysis.py

# Run all tests
python testing/data_source_analysis.py
python testing/live_data_endpoint_analysis.py
python testing/detailed_data_structure.py
python testing/extraction_data_testing.py
```

---

## Troubleshooting

### Scripts can't find utils/el_global.py
- Make sure you're running from project root
- Or use: `python /full/path/to/testing/script_name.py`

### Network timeouts
- Some endpoints may take 5-10 seconds
- Check your internet connection
- Try running again

### Missing dependencies
- Install: `pip install tabulate`
- Other modules (requests, pandas) should already be installed

---

## What to Do Next

1. **Understand your data:**
   - Run `data_source_analysis.py` to see the metadata
   - Run `live_data_endpoint_analysis.py` to see live data
   - Run `detailed_data_structure.py` for sample schemas

2. **Plan your pipeline:**
   - Decide: Use one or both metadata files? (Recommend: one)
   - Decide: Which endpoints do you need? (Likely: station_info + status)
   - Plan transformation strategy (BRONZE→SILVER→GOLD)

3. **Implement extraction:**
   - Update `scripts/1. extraction & loading/data_extraction.py`
   - Add transformation scripts in `scripts/2. transformations/`
   - Create analytics in gold layer

4. **Monitor & validate:**
   - Use comparison scripts to validate data consistency
   - Store results in BRONZE layer for audit trail
   - Track update frequencies and response times

---

## Document Index

- 📄 **DATA_SOURCE_EXPLANATION.md** - Detailed explanation of the data sources
- 📄 **README.md** - This file, testing scripts guide
- 📊 All Python scripts in this testing/ folder

---

Generated: 2026-02-20  
Last Updated: Testing suite complete with 4 comprehensive analysis scripts
