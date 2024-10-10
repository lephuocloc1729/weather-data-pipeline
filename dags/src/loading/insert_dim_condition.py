# Function to insert into dim_weather_condition
def insert_dim_weather_condition(cur, weather_condition_id, condition_main, condition_description):
    cur.execute("""
        INSERT INTO dim_weather_condition (weather_condition_id, condition_main, condition_description)
        VALUES (%s, %s, %s)
        ON CONFLICT (weather_condition_id) DO NOTHING 
    """, (weather_condition_id, condition_main, condition_description))
