import os
import psycopg
import sqlalchemy
import logging

logging.basicConfig(
    format='[%(process)d] %(levelname)s %(filename)s: %(message)s',
    level = logging.DEBUG)

_logger = logging.getLogger(__name__)


staging   = 'dev'
db_path   = os.path.join('credentials', staging, '.postgres.db')
psw_path  = os.path.join('credentials', staging, '.postgres.psw')
user_path = os.path.join('credentials', staging, '.postgres.user')
host_path = os.path.join('credentials', staging, '.postgres.host')
port_path = os.path.join('credentials', staging, '.postgres.port')


def extract_secret(path, default = ''):
    try:
        with open(path) as file:
            return file.read()
    except FileNotFoundError:
        return default


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

class ImportRegistry(Base):
    __tablename__ = 'import_registry'
    # We use ints instead of a foreign key because we are modelling
    # a record range instead of single pointers.
    start_id: Mapped[int] = mapped_column()
    end_id: Mapped[int] = mapped_column()

tables = [
    SaleOrder,
    ImportRegistry,
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

