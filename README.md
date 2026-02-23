# Automated Batch ETL Pipeline

---

Design and implement an end‑to‑end Bike Share Toronto analytics platform that ingests public GBFS and ridership data, models it with Kimball dimensional techniques, and evolves from a local batch warehouse to an automated, cloud‑native lakehouse.

### 🎯 Business Objective

---

### Core business questions

- How does Bike Share Toronto usage change by hour, day, and season?
- Which stations are heavily used or underutilized over time?
- What are typical trip durations and distances by user type and location?

### 📜 User stories

---

### ETL / data engineering

1. **Ingest raw trips**
    - As a data engineer, I want a Python script that fetches trip data from the API and loads it into bronze tables so that I have a raw, auditable copy of source data.
2. **Ingest station metadata and status**
    - As a data engineer, I want to ingest station lists and status snapshots so that I can model current and historical capacity and availability.
3. **Standardize and clean data (silver)**
    - As a data engineer, I want SQL models that clean types, time zones, and invalid records so that downstream tables are consistent and reliable.
4. **Build star schema (gold)**
    - As a data engineer, I want to populate dimension and fact tables using the Kimball model so that analysts can query data with simple joins.
5. **Surrogate keys and SCD handling**
    - As a data engineer, I want to generate surrogate keys and support Type 1 (later Type 2) station changes so that station history can be tracked correctly.
6. **Incremental loading**
    - As a data engineer, I want the pipeline to load only new and updated records since the last run so that daily refreshes are fast and efficient.
7. **Error handling and logging**
    - As a data engineer, I want detailed logs and an error table for rejected rows so that I can debug issues and monitor data quality.
8. **Re‑runnable pipeline**
    - As a data engineer, I want the ETL to be re‑runnable without creating duplicates so that failures can be recovered safely.
9. **Schema and lineage documentation**
    - As a data engineer, I want up‑to‑date documentation of tables, columns, and business rules so that new team members and analysts can use the data correctly.
10. **Airflow orchestration (Phase 2)**
    - As a data engineer, I want an Airflow DAG that chains extraction, loading, and transformations with retries and alerts so that the pipeline runs automatically each day.

### Analytics

These describe what an analyst or planner wants; they drive your dimensional design.wikipedia+1

1. **Daily ridership**
    - As a city planner, I want to see total trips per day so that I can understand seasonal and weekly demand patterns.
2. **Peak‑hour usage**
    - As an operations analyst, I want to see trips by hour of day so that I can identify peak usage periods and plan staffing and maintenance.
3. **Station performance**
    - As a planner, I want to compare trips starting and ending at each station so that I can classify stations as overused or underused.
4. **Trip duration and distance**
    - As a mobility analyst, I want distributions of trip duration and distance by user type so that I can understand how different users behave.
5. **Origin–destination flows**
    - As a planner, I want to see the most common station pairs so that I can identify important corridors and justify infrastructure improvements.
6. **Availability issues**
    - As an operations manager, I want to track how often stations are full or empty so that I can improve rebalancing and reduce service failures.

### Technology Stack

---

| **Component** | **Technology** | **Purpose** |
| --- | --- | --- |
| RDBMS | PostgreSQL | Store raw, cleaned, and star‑schema tables for the Bike Share warehouse. |
| Extraction & Loading (EL) | Python | Call the API, process responses, and load data into Postgres. |
| Transformations | SQL (PostgreSQL or dbt) | Clean, model, and aggregate data inside the database. |
| Source Data | Public REST API | Provide raw trip and station data for ingestion. |
| Environment & Runtime | Docker (optional) | Run Postgres and services in isolated, reproducible containers. |
| Logging & Monitoring | Python logging module | Record pipeline steps, errors, and run diagnostics. |
| Testing | Pytest | Validate ETL functions and basic data‑quality rules. |
| Version Control | Git / GitHub | Track code changes and collaborate on the project. |
| Configuration | YAML / .env files | Store API keys, DB URLs, and run parameters separately from code. |
| Documentation | Markdown, Draw.io | Describe architecture, tables, and pipeline flow with text and diagrams. |