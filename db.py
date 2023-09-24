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


#url = sqlalchemy.make_url(f'postgresql+psycopg://{user}:{psw}@{host}/{db}')
url = sqlalchemy.URL.create(
    drivername = 'postgresql+psycopg',
    username   = user,
    password   = psw,
    host       = host,
    database   = db,
)


# def dump(sql, *multiparams, **params):
#     _logger.info(sql.compile(dialect=engine.dialect))


from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import date

metadata_obj = sqlalchemy.MetaData()
class Base(DeclarativeBase):
    metadata = metadata_obj
    id: Mapped[int] = mapped_column(primary_key = True, sort_order = -1)

class SaleOrder(Base):
    __tablename__ = 'sale_order'

    PointOfSale: Mapped[str] = mapped_column()
    Product: Mapped[str] = mapped_column()
    Date: Mapped[date] = mapped_column()
    Stock: Mapped[int] = mapped_column()

tables = [
    SaleOrder,
]


# After analyzing the data, it seems that it has unique rows someway

from contextlib import contextmanager

@contextmanager
def session_builder():
    engine = sqlalchemy.create_engine(url)
    with sqlalchemy.orm.Session(engine) as session:
        metadata_obj.create_all(engine)
        yield session

if __name__ == '__main__':
    with session_builder() as session:
        statement = sqlalchemy.select(SaleOrder)
        print(statement)
        results = session.execute(statement)
        print(results)
        print(list(results))

