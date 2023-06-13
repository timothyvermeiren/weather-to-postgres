import os, sys, datetime, json, time, requests
from dotenv import load_dotenv
from getpass import getpass
from pathlib import Path
import logging, traceback
import logging.handlers # For RotatingFileHandler

load_dotenv()

# Timing
start_time = time.time()
start_datetime = datetime.datetime.utcnow()

# First thing, logs directory
logs_directory = Path("logs").absolute()
if not logs_directory.exists():
    os.makedirs(logs_directory)

##### Logging
logger = logging.getLogger() # Root logger
log_file = logs_directory / "tableau-cloud-reporter.log"
log_file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5000000, backupCount=5)
log_console_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")
log_formatter_console = logging.Formatter("%(asctime)s [%(levelname)s]  %(message)s")
log_file_handler.setFormatter(log_formatter)
log_console_handler.setFormatter(log_formatter_console)
logger.setLevel(logging.INFO)
logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)

logger.info("Weather to Postgres v1.0.0")
logger.info(f"Launched using (quotes removed): { sys.executable } { sys.argv[0] } { ' '.join([a if 'secret' not in sys.argv[i] else '****' for i, a in enumerate(sys.argv[1:])]) }") # Prints the command line used and tries to obfuscate arguments with "secret" in their name.

# POSTGRES

import psycopg2
import psycopg2.extras

# Postgres password from OS env
if os.environ.get("POSTGRES_PASSWORD") is None:
    print("The OS environment variable POSTGRES_PASSWORD has not been set. It's used to retrieve the Postgres password we use to connect. You can enter it now and have this script save it to a \"temporary\" environment variable, or exit with CTRL+C.")
    postgres_password = getpass()
else:
    postgres_password = os.environ["POSTGRES_PASSWORD"]

drivername="postgres"
try:
    postgres_username = os.environ["POSTGRES_USERNAME"]
    postgres_password = postgres_password
    postgres_host = os.environ["POSTGRES_HOST"]
    postgres_port = os.environ["POSTGRES_PORT"]
    postgres_database = os.environ["POSTGRES_DATABASE"]
    postgres_schema = os.environ["POSTGRES_SCHEMA"]
    postgres_url = f"{drivername}://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
except Exception as e:
    logger.error(f"Something went wrong assimilating the postgres connection information we need (from OS environment variables).\n{e}")
    sys.exit(1)

logger.info(f"Connecting to postgres database at { postgres_host }.")

try:
    connection = psycopg2.connect(postgres_url)
    cur = connection.cursor()
except Exception as e:
    logger.error(f"Something went wrong connecting to the postgres database at host \"{ postgres_host }\".\n{e}")
    sys.exit(1)

logger.info(f"Postgres database connected successfully.")

# WEATHER
# owm = Open Weather Map

if os.environ.get("OPENWEATHERMAP_API_KEY") is None:
    print("The OS environment variable OPENWEATHERMAP_API_KEY has not been set. It's used to... yeah, you can guess what for. You can enter it now and have this script save it to a \"temporary\" environment variable, or exit with CTRL+C.")
    owm_api_key = getpass()
else:
    owm_api_key = os.environ["OPENWEATHERMAP_API_KEY"]

try:
    owm_lat = os.environ["OPENWEATHERMAP_LAT"]
    owm_lon = os.environ["OPENWEATHERMAP_LON"]
except Exception as e:
    logger.error(f"Something went wrong assimilating the Open Weather Map information we need (from OS environment variables). Coordinates, actually.\n{e}")
    sys.exit(1)

owm_base_url = "https://api.openweathermap.org/data/2.5"
owm_request_url = f"{ owm_base_url }/forecast?lat={ owm_lat }&lon={ owm_lon }&appid={ owm_api_key }"

owm_response = requests.get(url=owm_request_url)

connection.close()
sys.exit(0)