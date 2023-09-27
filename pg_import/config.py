import os
import sqlalchemy
import logging

logging.basicConfig(
    format = '[%(process)d] %(levelname)s %(filename)s: %(message)s',
    level  =  logging.DEBUG)

def extract_secret(path, default = ''):
    try:
        with open(path) as file:
            return file.read()
    except FileNotFoundError:
        return default

# Staging Resolution:
#   1. Prioritize the stagging from the environment variable $PG_IMPORT_STAGING is set.
#   2. If no env. variable is set, look try to load the stagging from the file $PWD/.stagging.
# This scheme let us having a connection scheme for stagging/branch.

config_path  = os.environ.get('PG_IMPORT_CONFIG_PATH', 'credentials')
staging      = os.environ.get('PG_IMPORT_STAGING')
staging_path = os.path.join(os.getcwd(), '.staging')

if not staging:
    staging = extract_secret(staging_path, 'dev')

# Load the data stored in each secret file:
db_path   = os.path.join(config_path, staging, '.postgres.db')
psw_path  = os.path.join(config_path, staging, '.postgres.psw')
user_path = os.path.join(config_path, staging, '.postgres.user')
host_path = os.path.join(config_path, staging, '.postgres.host')
port_path = os.path.join(config_path, staging, '.postgres.port')


# Database connection data:
db   = extract_secret(db_path   , 'postgres')
psw  = extract_secret(psw_path  , 'psw')
user = extract_secret(user_path , 'admin')
host = extract_secret(host_path , 'localhost')
port = extract_secret(port_path , '5432')
url  = sqlalchemy.URL.create(
    drivername = 'postgresql+psycopg',
    username   = user,
    password   = psw,
    host       = host,
    port       = port,
    database   = db,
)

# NOTE: Not required anymore
del psw
