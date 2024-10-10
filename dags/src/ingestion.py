import requests
from src.config import API_KEY


def fetch_weather_data():
    city = "New York"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
    print(url)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching weather data: {response.status_code}")
