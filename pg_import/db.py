"""
    Creates the models used by the application.
    Also, gives an unified connection interface to the database.
"""
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
    """
        Represents the actual data that we want to import into the database.
    """
    __tablename__ = 'sale_order'

    PointOfSale: Mapped[str] = mapped_column()
    Product: Mapped[str] = mapped_column()
    Date: Mapped[date] = mapped_column()
    Stock: Mapped[int] = mapped_column()


# NOTE: We could improve the information provided by this model by adding
#       a run_id attribute or even by adding a timestamp.
# NOTE: We can add an attribute called table/model later so it can represent
#       import ranges from different tables. but to keep this example trivial,
#       we just assume that the imported range belongs to SaleOrder.
# NOTE: By adding this 
class ImportRegistry(Base):
    """
        Holds closed ranges of records imported in the table SaleOrder.
    """
    __tablename__ = 'import_registry'

    # We use ints instead of a FK because we are modelling closed ranges [start_id, end_id]
    # instead of just referring to a single record per row.
    start_id: Mapped[int] = mapped_column()
    end_id: Mapped[int] = mapped_column()


@contextmanager
def session_builder():
    """
        Provides an unified session interface for connecting to the database.
    """
    engine = sqlalchemy.create_engine(url)
    with sqlalchemy.orm.Session(engine) as session:
        metadata_obj.create_all(engine)
        yield session
