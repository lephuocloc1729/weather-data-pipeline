from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from src import fetch_weather_data, process_and_load_weather_data
from airflow.utils.dates import days_ago

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'fetch_weather_data_dag',
    default_args=default_args,
    description='A simple DAG to fetch and load weather data',
    schedule_interval='@hourly',
    catchup=False,
)

create_dim_location_table = PostgresOperator(
    task_id='create_dim_location_table',
    postgres_conn_id='my_local_postgres',  # Replace with your connection ID
    sql="""
        CREATE TABLE IF NOT EXISTS dim_location (
            city_id SERIAL PRIMARY KEY,
            city_name VARCHAR(50),
            country_code VARCHAR(10),
            lat FLOAT,
            lon FLOAT,
            timezone VARCHAR(255),
            timezone_offset INT
        );
    """,
    dag=dag,
)

create_dim_weather_condition_table = PostgresOperator(
    task_id='create_dim_weather_condition_table',
    postgres_conn_id='my_local_postgres',  # Replace with your connection ID
    sql="""
        CREATE TABLE IF NOT EXISTS dim_weather_condition (
            weather_condition_id SERIAL PRIMARY KEY,
            condition_main VARCHAR(50),
            condition_description VARCHAR(255)
        );
    """,
    dag=dag,
)

create_fact_weather_table = PostgresOperator(
    task_id='create_fact_weather_table',
    postgres_conn_id='my_local_postgres',  # Replace with your connection ID
    sql="""
        CREATE TABLE IF NOT EXISTS fact_weather (
            city_id INT REFERENCES dim_location(city_id),
            weather_condition_id INT REFERENCES dim_weather_condition(weather_condition_id),
            timestamp TIMESTAMP,
            sunrise TIMESTAMP,
            sunset TIMESTAMP,
            temp FLOAT,
            feels_like FLOAT,
            pressure INT,
            humidity INT,
            clouds INT,
            uvi FLOAT,
            visibility INT,
            wind_speed FLOAT,
            wind_gust FLOAT,
            wind_deg INT,
            PRIMARY KEY (city_id, timestamp)
        );
    """,
    dag=dag,
)


fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

process_weather_task = PythonOperator(
    task_id='process_and_load_weather_data',
    python_callable=process_and_load_weather_data,
    dag=dag,
)

# Set task dependencies
[create_dim_location_table, create_dim_weather_condition_table,
    create_fact_weather_table] >> fetch_weather_task >> process_weather_task
