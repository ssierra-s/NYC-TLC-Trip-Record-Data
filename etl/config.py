import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "taxi_dw")
DB_USER = os.getenv("POSTGRES_USER", "taxi_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxi_password")

TAXI_TYPE = os.getenv("TAXI_TYPE", "yellow")
START_YEAR = int(os.getenv("START_YEAR", 2024))
START_MONTH = int(os.getenv("START_MONTH", 1))
MONTHS_TO_PROCESS = int(os.getenv("MONTHS_TO_PROCESS", 3))
