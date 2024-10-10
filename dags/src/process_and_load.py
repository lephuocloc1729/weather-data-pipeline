import psycopg2
from src.insertion.insert_dim_condition import insert_dim_weather_condition
from src.insertion.insert_dim_location import insert_dim_location
from src.insertion.insert_fact_weather import insert_fact_weather
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

        # Extract weather condition and insert into dim_weather_condition
        weather_condition = data['weather'][0]
        condition_id = weather_condition['id']
        condition_main = weather_condition['main']
        condition_description = weather_condition['description']

        insert_dim_weather_condition(
            cur, condition_id, condition_main, condition_description)

        # Extract fact weather data for fact_weather table
        timestamp = datetime.utcfromtimestamp(data['dt'])
        sunrise = datetime.utcfromtimestamp(data['sys']['sunrise'])
        sunset = datetime.utcfromtimestamp(data['sys']['sunset'])
        temp = data['main']['temp']
        feels_like = data['main']['feels_like']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        clouds = data['clouds']['all']
        visibility = data['visibility']
        wind_speed = data['wind']['speed']
        wind_gust = data['wind'].get('gust', None)
        wind_deg = data['wind']['deg']

        # Prepare fact_weather data
        insert_fact_weather(
            cur, city_id, condition_id, timestamp, sunrise, sunset,
            temp, feels_like, pressure, humidity,
            clouds, visibility, wind_speed, wind_gust, wind_deg
        )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")
