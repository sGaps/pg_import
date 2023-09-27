"""
    Groups useful information that can by used by other python modules
    defined in this package.

    The module tries to load the configuration parameters from files that
    contains credentials, connection settings and misc. information. The
    files should be in a directory that must have the following structure:
        {credentials}/
            {staging}/
                secret01
                secret02
    
    Where the fragment {credentials} is a path that can be set by the
    environment variable $PG_IMPORT_CONFIG_PATH. It's default value is
    'credentials/'. This path must contain a folder called {staging}
    which can be set by the environment variable $PG_IMPORT_STAGING
    or by a file contained in $PWD/.staging. 

    When {stagging} is not set, the value 'dev' is assumed. The
    files that are currently loaded from {credentials}/{stagging}/
    are:
        - .postgres.db
        - .postgres.psw
        - .postgres.user
        - .postgres.port
        - .postgres.host
"""

import os
import sqlalchemy
import logging

logging.basicConfig(
    format = '[%(process)d] %(levelname)s %(filename)s: %(message)s',
    level  =  logging.DEBUG)

def extract_secret(path, default = ''):
    """
        Extract the contents of a file or return a default value when the path doesn't exist.
    """
    try:
        with open(path) as file:
            return file.read()
    except FileNotFoundError:
        return default

# Staging Resolution:
#   1. Prioritize the staging from the environment variable $PG_IMPORT_STAGING is set.
#   2. If no env. variable is set, look try to load the staging from the file $PWD/.staging.
# This scheme let us having a connection scheme for staging/branch.

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
