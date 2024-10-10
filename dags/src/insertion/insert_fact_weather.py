def insert_fact_weather(
    cur, city_id, condition_id, timestamp, sunrise, sunset,
    temp, feels_like, pressure, humidity,
    clouds, visibility, wind_speed, wind_gust, wind_deg
):
    cur.execute("""
            INSERT INTO fact_weather (
                city_id, weather_condition_id, timestamp, sunrise, sunset, temp, feels_like, 
                pressure, humidity, clouds, visibility, wind_speed, wind_gust, wind_deg
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
        city_id,
        condition_id,
        timestamp,
        sunrise,
        sunset,
        temp,
        feels_like,
        pressure,
        humidity,
        clouds,
        visibility,
        wind_speed,
        wind_gust,
        wind_deg
    ))
