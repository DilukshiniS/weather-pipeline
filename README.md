# Multi-City Weather Data Pipeline

An end-to-end ELT pipeline: Open-Meteo API → Airflow → Snowflake → dbt

## Business Question
Which cities are hottest and rainiest? How do temperatures compare across
Colombo, London, New York, Tokyo, and Dubai over time?

## Tech Stack
- **Airflow + Cosmos** — Orchestration and dbt task management
- **Snowflake** — Cloud data warehouse (RAW / STAGING / MART layers)
- **dbt** — SQL transformations with tests and documentation
- **Docker** — Containerised environment

## Architecture
Open-Meteo API → Airflow DAG (Docker) → Snowflake RAW → dbt Staging → dbt Mart