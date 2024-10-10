# Function to insert into dim_location
def insert_dim_location(cur, city_id, city_name, country_code, lat, lon, timezone, timezone_offset):
    cur.execute("""
        INSERT INTO dim_location (city_id, city_name, country_code, lat, lon, timezone, timezone_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id) DO NOTHING 
    """, (city_id, city_name, country_code, lat, lon, timezone, timezone_offset))
