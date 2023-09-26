# INPUT
#   a csv file or stdin (they only need csv file really).
#       This file has rows that must be inserted into a local
#       PostgreSQL database

# take the rows and insert it into the database.
# NOTE: you must remove the content from a possible previous import.

# Delivery method:
#   Put the code in a public Github Repository.

import csv
from itertools import islice

import psycopg
import sqlalchemy

from db import session_builder, SaleOrder, ImportRegistry
from cli import cli_parser, manage_input, attach_signals

import logging
_logger = logging.getLogger(__name__)

# CHUNK_SIZE = 1_000_000 # TIME: 12m 27s, RAM: 3   GiB ~ 4GiB
# CHUNK_SIZE = 100_000   # TIME: 12m 14s, RAM: 410 MiB ~ 450MiB
# CHUNK_SIZE = 1_000     # TIME: 13n 05s, RAM: 53  MiB ~ 53 MiB
# CHUNK_SIZE = 5         # TIME: <TESTING METHOD>
CHUNK_SIZE = 10_000      # TIME: 12m 06s, RAM: 93  MiB ~ 93MiB (OPTIMAL)

# SEE ALSO: https://docs.python.org/3/library/itertools.html#itertools-recipes
def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

def preliminar_delete_import_range(session):
    to_delete = session.scalars(sqlalchemy.select(ImportRegistry).order_by(ImportRegistry.id))
    #with session.begin_nested():
    unsafe_delete_import_range(session, to_delete)
    session.execute(sqlalchemy.delete(ImportRegistry))

    _logger.info('COMPLETED DELETION PROCESS')


def rollback_import(session, to_delete):
    # with session.begin_nested():
    unsafe_delete_import_range(session, to_delete)
    for record in to_delete:
        _logger.info(f"Deleting: {record.start_id} ... {record.end_id} (size: {record.end_id - record.start_id + 1} elements)")
        session.delete(record)
    _logger.info('COMPLETED DELETION PROCESS')
    _logger.warning('Remember to vacuum your database later')


def unsafe_delete_import_range(session, to_delete):
    cursor = psycopg.ClientCursor(session.connection().connection)

    for i, chunk_to_delete in enumerate( batched(to_delete, CHUNK_SIZE) ):
        _logger.info(f'Deleting batch no. {i} (DELETED: {i * CHUNK_SIZE} VALUES)')

        delete_query = "DELETE FROM sale_order WHERE "

        delete_template = 'id >= %s AND id <= %s\n'

        delete_query += ' OR '.join(
                cursor.mogrify(delete_template, [record.start_id, record.end_id])
                for record
                in chunk_to_delete)

        cursor.execute(
            delete_query,
        )


# TODO: pass import_range.
# TODO: Remove external dependency of current_import_range
def unsafe_import_records(session, chunk, ranges_inserted):
    # NOTE: usage of raw psycopg for efficiency
    cursor = psycopg.ClientCursor(session.connection().connection)

    insert_query = """
        WITH tmp AS (INSERT INTO
            sale_order("PointOfSale", "Product", "Date", "Stock")
        VALUES """

    insert_template = "(%s, %s, %s, %s)"

    insert_query += ','.join(
            cursor.mogrify(insert_template, line)
            for line
            in chunk)

    insert_query += """
        RETURNING id)
        SELECT min(tmp.id) AS start_id, max(tmp.id) AS end_id FROM tmp;
    """

    cursor.execute(
        insert_query,
    )

    row = cursor.fetchone()
    if row:
        start_id = row[0]
        end_id = row[1]

        current_import_range = ranges_inserted and ranges_inserted[-1]

        if (current_import_range and start_id == current_import_range.end_id + 1):
            # WE ARE STILL IN THE SAME RANGE, SO WE CAN JOIN THEM IN PLACE
            current_import_range.end_id = end_id
        else:
            # we register a new range into our range pool because it is the
            # first range one we are building or because we encountered a non-
            # perfectly consecutive range, meaning that another process inserted
            # data in the table.
            current_import_range = ImportRegistry(
                start_id = start_id,
                end_id = end_id)
            session.add(current_import_range)
            ranges_inserted.append(current_import_range)
    else:
        _logger.warning("Records couldn't be inserted on batch no.", i)


def main():
    parser = cli_parser()
    args   = parser.parse_args()

    stream    = manage_input(args.input)
    delimiter = args.delimiter

    with stream as file, session_builder() as session:
        reader = csv.reader(file, delimiter = delimiter)
        # reader = csv.DictReader(file, delimiter = delimiter)

        # NOTE: for now, we assume that we always have a header inside the csv files
        # NOTE: We can use DictReader, but it assumes that the header always have
        #       well written names, but in our case, the first column name has an invisible
        #       utf-8 character that would lead to undefined behavior, so we must clean
        #       the names before translating these to 
        # NOTE: We could use DictReader, but it will imply a
        header = next(reader)
        _logger.info(f'Retrieved Header: {header}')

        with session.begin():
            preliminar_delete_import_range(session)

        ranges_inserted = [] 
        try:
            attach_signals()
            for i, chunk in enumerate(batched(reader, CHUNK_SIZE)):
                with session.begin():
                    _logger.info(f'Starting batch no. {i} (SENT: {i * CHUNK_SIZE} VALUES)')
                    unsafe_import_records(session, chunk, ranges_inserted)
            _logger.info('Import process complete!')
        except (InterruptedError, psycopg.Error) as err:
            with session.begin():
                rollback_import(session, ranges_inserted)
            raise

main()