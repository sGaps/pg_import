import os

staging   = 'dev'
db_path   = os.path.join('credentials', staging, '.postgres.db')
psw_path  = os.path.join('credentials', staging, '.postgres.psw')
user_path = os.path.join('credentials', staging, '.postgres.user')
host_path = os.path.join('credentials', staging, '.postgres.host')

def extract_secret(path):
    with open(path) as file:
        return file.read()


db   = extract_secret(db_path)
psw  = extract_secret(psw_path)
user = extract_secret(user_path)
host = extract_secret(host_path)

import psycopg
import sqlalchemy
import logging


logging.basicConfig(level = logging.DEBUG)
_logger = logging.getLogger(__name__)


def dump(sql, *multiparams, **params):
    _logger.info(sql.compile(dialect=engine.dialect))

url = sqlalchemy.make_url(f'postgresql+psycopg://{user}:{psw}@{host}/{db}')
engine = sqlalchemy.create_engine(url)
with engine.connect() as conn:
    print(conn)

