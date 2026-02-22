# Data Source Analysis Report

## Executive Summary

You asked: **"Which data is live, and why is it parsed into two file names?"**

**Answer:** The two files (`bike-share-json` and `gbfs-specification`) are **metadata files** pointing to the **same live data source**. They contain URLs to actual bike share data endpoints.

---

## Key Findings

### 1. **Both Files Point to IDENTICAL URLs**
- ✓ `system_regions` → Same URL in both
- ✓ `system_information` → Same URL in both
- ✓ `station_information` → Same URL in both
- ✓ `station_status` → Same URL in both
- ✓ `system_pricing_plans` → Same URL in both

### 2. **Why Two Separate Files?**

The two metadata files appear to be extracted from the **same source at nearly the same time** (0.15 seconds apart):

| Aspect | bike-share-json | gbfs-specification |
|--------|-----------------|-------------------|
| Timestamp | 2026-02-18T03:38:34.213433 | 2026-02-18T03:38:34.365408 |
| Feeds Count | 5 | 5 |
| URL Endpoints | Identical | Identical |
| Data Status | All OK | All OK |

**Likely Reasons for Dual Extraction:**
1. **Format Validation**: Testing compliance with GBFS specification
2. **Quality Assurance**: Comparing data consistency between two extraction methods
3. **Pipeline Testing**: Validating that both extraction paths work correctly
4. **Data Schema Compliance**: Ensuring data meets standards

### 3. **The ACTUAL Live Data**

The real live data is fetched from these endpoints (all working):

- **station_information**: 510.34 KB - List of all bike stations
- **station_status**: 336.78 KB - Real-time bike/dock availability
- **system_pricing_plans**: 8.43 KB - Pricing information
- **system_information**: 0.42 KB - System metadata
- **system regions**: 0.06 KB - Geographic regions

These files are **snapshots** - they are updated on each batch run (evidenced by different timestamps in each extraction).

---

## Data Architecture

```
┌─────────────────────────────────────────────┐
│        LIVE TORONTO BIKE SHARE API          │
│  https://tor.publicbikesystem.net/          │
│      (Updated every few seconds)            │
└─────────┬───────────────────────────────────┘
          │
    ┌─────┴─────┐
    │ EXTRACTION │ (Happens at scheduled intervals)
    └─────┬─────┘
          │
    ┌─────┴──────────────────────────────────────┐
    │   METADATA FILES (feeds_summary.json)       │
    │   ├─ bike-share-json snapshot               │
    │   └─ gbfs-specification snapshot            │
    │   • Both contain URLs to live endpoints     │
    │   • Both extracted within 0.15 seconds      │
    └─────┬──────────────────────────────────────┘
          │
    ┌─────┴──────────────────────────────────────┐
    │   YOUR PROCESSING PIPELINE                 │
    │                                             │
    ├─ BRONZE (storage/raw_data):               │
    │  • Store both metadata snapshots            │
    │  • Store live endpoint data                 │
    │  • Keep full history                        │
    │                                             │
    ├─ SILVER (transformations):                │
    │  • Parse JSON → Structured format           │
    │  • Data validation/cleansing                │
    │  • Schema standardization                   │
    │                                             │
    └─ GOLD (analytics):                        │
       • Station utilization metrics             │
       • Regional statistics                     │
       • Pricing analysis                        │
       • Business intelligence                   │
```

---

## File Structure Explanation

### **feeds_summary.json** (Metadata File - LIGHTWEIGHT)
```json
{
  "timestamp": "2026-02-18T03:38:34.213433",
  "resource": "bike-share-json",
  "feeds_count": 5,
  "feeds": {
    "station_information": {
      "url": "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information",
      "has_data": true,
      "error": null
    },
    // ... other endpoints
  }
}
```
- **Size**: ~1-2 KB
- **Purpose**: Discovery/validation
- **Frequency**: Extracted with each batch job
- **Use**: Find out which endpoints are available and working

### **Actual Data** (Retrieved via URLs - HEAVYWEIGHT)
```json
{
  "last_updated": 1771577103,
  "ttl": 7,
  "data": {
    "stations": [
      {
        "station_id": "123",
        "name": "Bloor St & Avenue Rd",
        "lat": 43.667,
        "lon": -79.395,
        "capacity": 23,
        // ... station details
      },
      // ... 700+ more stations
    ]
  }
}
```
- **Size**: 300+ KB
- **Purpose**: Actual bike share operational data
- **Frequency**: Updated every 5-30 seconds
- **Use**: Analytics, real-time dashboards, reporting

---

## Why Both Get Extracted?

| Scenario | Reason |
|----------|--------|
| **Testing** | Verify both extraction methods work |
| **Validation** | Ensure GBFS compliance |
| **Monitoring** | Track API health/availability |
| **Comparison** | Detect data inconsistencies |
| **Backup** | Have redundant extraction paths |

---

## Recommendations for Your Project

### **Short Term**
1. ✓ You can safely process just **one metadata file** (they're identical)
2. ✓ Use the metadata to discover the live endpoint URLs
3. ✓ Fetch actual data from those endpoints (station_information, station_status, etc.)

### **Medium Term**
1. Create a scheduler for batch extraction (current timestamps show one was already done)
2. Store metadata in BRONZE layer for audit trail
3. Transform and aggregate in SILVER/GOLD layers

### **Long Term**
1. Set up monitoring alerts if endpoints become unavailable
2. Track data quality metrics over time
3. Analyze patterns in bike availability and usage

---

## Next Actions

Check out these testing scripts in the `testing/` folder:

1. **`data_source_analysis.py`** - Metadata comparison (structure, timestamps, status)
2. **`live_data_endpoint_analysis.py`** - Fetch and analyze actual live data
3. **`extraction_data_testing.py`** - Compare JSON differences

All scripts use the `el_global.py` module and can be run from any location in the project:

```bash
# From project root
python testing/data_source_analysis.py
python testing/live_data_endpoint_analysis.py
```

---

## Questions Answered

**Q: Which one is live data?**
- **A:** Both point to the same live data. The actual live data is at the URLs they contain.

**Q: Why two file names?**
- **A:** Likely testing/validation of different extraction methods or formats.

**Q: Can I use just one?**
- **A:** Yes, they contain identical information. Pick one and use it as your metadata source.

**Q: How often is it updated?**
- **A:** Timestamps show the files are created on each batch run (~0.15 seconds apart).
- **A:** Actual station data is updated every 5-30 seconds at the source.

**Q: Where's the REAL data?**
- **A:** In the `data` objects returned from the live endpoints (station_information~510KB, station_status~337KB).

---

## Data Schema Summary

| Endpoint | Size | Contents | Update Frequency |
|----------|------|----------|------------------|
| system_regions | 0.06 KB | Geographic regions | Rarely changed |
| system_information | 0.42 KB | System metadata (name, location, language) | Daily |
| station_information | 510 KB | All stations with lat/lon/capacity | Daily |
| station_status | 337 KB | Real-time bikes/docks at each station | Every 5-30s |
| system_pricing_plans | 8.43 KB | Pricing tiers and plans | As needed |

**Total Live Data**: ~856 KB per extraction

---

Generated: 2026-02-20  
Analysis Scripts Location: `/testing/`  
Module: `utils/el_global.py`
