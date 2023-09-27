from datetime import date
from contextlib import contextmanager

import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .config import url


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


# NOTE: We could improve the information provided by this model by adding
#       a run_id attribute or even by adding a timestamp.
class ImportRegistry(Base):
    __tablename__ = 'import_registry'
    # We use ints instead of a foreign key because we are modelling
    # a record range instead of single pointers.
    start_id: Mapped[int] = mapped_column()
    end_id: Mapped[int] = mapped_column()


@contextmanager
def session_builder():
    engine = sqlalchemy.create_engine(url)
    with sqlalchemy.orm.Session(engine) as session:
        metadata_obj.create_all(engine)
        yield session
