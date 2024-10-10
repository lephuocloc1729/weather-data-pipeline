import psycopg2
from src.loading.insert_dim_condition import insert_dim_weather_condition
from src.loading.insert_dim_location import insert_dim_location
from src.create_schema import create_schema
from datetime import datetime
from src.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def process_and_load_weather_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_weather_data')
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        # create_schema(conn)

        cur = conn.cursor()

        # Extract location data
        city_id = data['id']
        city_name = data['name']
        country_code = data['sys']['country']
        lat = data['coord']['lat']
        lon = data['coord']['lon']
        timezone = data['timezone']
        timezone_offset = data['timezone']

        # Insert into dimension tables
        insert_dim_location(cur, city_id, city_name, country_code,
                            lat, lon, timezone, timezone_offset)

        # Extract weather condition
        weather_condition = data['weather'][0]
        condition_id = weather_condition['id']
        condition_main = weather_condition['main']
        condition_description = weather_condition['description']

        # Insert into dim_weather_condition
        insert_dim_weather_condition(
            cur, condition_id, condition_main, condition_description)

        # Insert into fact_weather
        cur.execute("""
            INSERT INTO fact_weather (
                city_id, weather_condition_id, timestamp, sunrise, sunset, temp, feels_like, 
                pressure, humidity, clouds, visibility, wind_speed, wind_gust, wind_deg
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            city_id,
            condition_id,
            datetime.fromtimestamp(data['dt']),
            datetime.fromtimestamp(
                data['sys']['sunrise']),
            datetime.fromtimestamp(
                data['sys']['sunset']),
            data['main']['temp'],
            data['main']['feels_like'],
            data['main']['pressure'],
            data['main']['humidity'],
            # data['main']['dew_point'],
            data['clouds']['all'],
            data['visibility'],
            data['wind']['speed'],
            data['wind'].get('gust', None),
            data['wind']['deg']
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")
