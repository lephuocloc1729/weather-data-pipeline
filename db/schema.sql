-- Dimension table: dim_location
CREATE TABLE dim_location (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(50),
    country_code VARCHAR(10),
    lat FLOAT,
    lon FLOAT,
    timezone VARCHAR(255),
    timezone_offset INT
);

-- Dimension table: dim_weather_condition
CREATE TABLE dim_weather_condition (
    weather_condition_id SERIAL PRIMARY KEY,
    condition_main VARCHAR(50),
    condition_description VARCHAR(255)
);

-- Fact table: fact_weather
CREATE TABLE fact_weather (
    weather_id SERIAL PRIMARY KEY,
    city_id INT REFERENCES dim_location(city_id),
    weather_condition_id INT REFERENCES dim_weather_condition(weather_condition_id),
    timestamp TIMESTAMP,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    temp FLOAT,
    feels_like FLOAT,
    pressure INT,
    humidity INT,
    dew_point FLOAT,
    clouds INT,
    uvi FLOAT,
    visibility INT,
    wind_speed FLOAT,
    wind_gust FLOAT,
    wind_deg INT,
    rain_1h FLOAT,
    snow_1h FLOAT
);
