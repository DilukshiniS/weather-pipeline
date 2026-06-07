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

## How to Run
1. Clone this repo
2. Copy `.env.example` to `.env` and fill in your Snowflake credentials
3. Run `docker compose up`
4. Open `http://localhost:8080` (user: admin, pass: admin)
5. Toggle on `multi_city_weather_pipeline` and trigger it

## dbt Docs
Run inside Docker to view the lineage graph:
```bash
docker compose exec airflow bash -c "cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8081"
```
Then open `http://localhost:8081`

## Project Structure
- `dags/` — Airflow DAG definition
- `dbt/models/staging/` — Cleaning models
- `dbt/models/mart/` — Analytics models
- `ddl/` — Raw table DDL SQL
- `docs/` — Sample API response and notes