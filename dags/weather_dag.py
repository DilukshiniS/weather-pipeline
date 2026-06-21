from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime, timedelta
import requests
import os
import snowflake.connector
import logging

# ─────────────────────────────────────────────────────────────────────────────
# CITY LIST
# Add or remove cities here. Find coordinates at https://www.latlong.net/
# ─────────────────────────────────────────────────────────────────────────────
CITIES = [
    {"name": "Colombo",   "country": "Sri Lanka",      "latitude": 6.9271,  "longitude": 79.8612},
    {"name": "London",    "country": "United Kingdom",  "latitude": 51.5074, "longitude": -0.1278},
    {"name": "New York",  "country": "United States",   "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo",     "country": "Japan",           "latitude": 35.6762, "longitude": 139.6503},
    {"name": "Dubai",     "country": "UAE",             "latitude": 25.2048, "longitude": 55.2708},
]

# ─────────────────────────────────────────────────────────────────────────────
# ALERT FUNCTION
# This function is called automatically by Airflow when any task fails.
# It sends an email with details about what failed and why.
# ─────────────────────────────────────────────────────────────────────────────
def alert_on_failure(context):
    """
    Called automatically by Airflow on task failure.
    'context' is a dictionary Airflow passes in with details about the failed task.
    """
    alert_email = os.environ.get('ALERT_EMAIL', '')
    if not alert_email:
        logging.warning("ALERT_EMAIL not set — skipping failure notification")
        return

    # Build a readable subject and body
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    subject = f"[Airflow FAILED] {dag_id} → {task_id}"
    body = f"""
    <h3>Pipeline Failure Alert</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Run date:</strong> {execution_date}</p>
    <p><strong>Error:</strong> {exception}</p>
    <p>Open Airflow at <a href="http://localhost:8080">http://localhost:8080</a> to see the full logs.</p>
    """

    send_email(to=alert_email, subject=subject, html_content=body)
    logging.info(f"Failure alert sent to {alert_email}")

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGUMENTS
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': alert_on_failure,
}

# ─────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE CONNECTION HELPER
# ─────────────────────────────────────────────────────────────────────────────
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
    )
    return conn

# ─────────────────────────────────────────────────────────────────────────────
# TASK 1: FETCH WEATHER DATA
# ─────────────────────────────────────────────────────────────────────────────
def fetch_weather(**context):
    logging.info(f"Fetching weather for {len(CITIES)} cities...")
    all_city_data = []

    for city in CITIES:
        logging.info(f"Calling API for {city['name']}...")
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "auto",
            "forecast_days": 7
        }
        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise Exception(
                f"API failed for {city['name']}. "
                f"Status: {response.status_code}. Body: {response.text}"
            )

        api_data = response.json()
        all_city_data.append({
            "city_name": city["name"],
            "country": city["country"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "daily": api_data["daily"],
        })
        logging.info(f"Got {len(api_data['daily']['time'])} days for {city['name']}")

    context['ti'].xcom_push(key='all_city_weather', value=all_city_data)
    return f"Fetched {len(all_city_data)} cities"

# ─────────────────────────────────────────────────────────────────────────────
# TASK 2: LOAD TO SNOWFLAKE
# ─────────────────────────────────────────────────────────────────────────────
def load_to_snowflake(**context):
    logging.info("Loading data to Snowflake...")
    all_city_data = context['ti'].xcom_pull(task_ids='fetch_weather', key='all_city_weather')

    if not all_city_data:
        raise Exception("No data from fetch_weather task. Check XCom.")

    today = datetime.now().date()
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Delete existing rows for today to prevent duplicates
    cursor.execute("DELETE FROM WEATHER_RAW WHERE LOAD_DATE = %s", (today,))
    logging.info(f"Cleared {cursor.rowcount} existing rows for {today}")

    total_rows = 0
    for city_data in all_city_data:
        daily = city_data["daily"]
        dates = daily["time"]
        temps_max = daily["temperature_2m_max"]
        temps_min = daily["temperature_2m_min"]
        precipitation = daily["precipitation_sum"]

        for i in range(len(dates)):
            cursor.execute("""
                INSERT INTO WEATHER_RAW
                    (LOAD_DATE, CITY_NAME, COUNTRY, LATITUDE, LONGITUDE,
                     WEATHER_DATE, TEMP_MAX, TEMP_MIN, PRECIPITATION)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                today,
                city_data["city_name"],
                city_data["country"],
                city_data["latitude"],
                city_data["longitude"],
                dates[i],
                temps_max[i],
                temps_min[i],
                precipitation[i] if precipitation[i] is not None else 0.0,
            ))
            total_rows += 1

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Inserted {total_rows} rows for {len(all_city_data)} cities")
    return f"Loaded {total_rows} rows"

# ─────────────────────────────────────────────────────────────────────────────
# COSMOS CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
DBT_PROJECT_PATH = "/opt/airflow/dbt"

profile_config = ProfileConfig(
    profile_name="weather_project",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "WEATHER_DB",
            "warehouse": "WEATHER_WH",
            "schema": "RAW",
            "role": "pipeline_role",
        },
    ),
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='multi_city_weather_pipeline',
    default_args=default_args,
    description='Multi-city weather ELT: Open-Meteo → Snowflake → dbt (via Cosmos)',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['weather', 'snowflake', 'dbt', 'cosmos'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather,
        provide_context=True,
    )

    task_load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        default_args=default_args,
    )

    task_fetch >> task_load >> dbt_transform