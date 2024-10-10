import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('/opt/airflow/.env')

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# API KEY
API_KEY = os.getenv('API_KEY')