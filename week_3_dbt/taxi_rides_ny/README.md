# NYC Taxi Data Transformation Pipeline (dbt + Postgres)

## Project Overview
This project is part of the Data Engineering Zoomcamp (Week 3). It builds a transformation pipeline for NYC Taxi data (Yellow and Green) using **dbt Core** and **Postgres**.

## Architecture
1.  **Ingestion:** Python scripts load raw CSV data into a local Postgres database.
2.  **Staging:** dbt models (`stg_green_tripdata`, `stg_yellow_tripdata`) clean and standardize the data.
3.  **Core:** A Star Schema is built with `dim_zones` and `fact_trips`.
4.  **Testing:** Unique ID constraints and non-null tests ensure data integrity.

## Key Features
* **Surrogate Keys:** Composite keys generated to resolve ID collisions between Yellow/Green taxi vendors.
* **Macros:** DRY principles applied for payment type translation.
* **Data Quality:** Automated `dbt test` suite passing 7/7 checks.

## Tech Stack
* **dbt Core:** 1.8+
* **Database:** Postgres 13 (Dockerized)
* **Containerization:** Docker Compose
