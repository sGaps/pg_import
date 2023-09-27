import csv
from itertools import islice

import psycopg
import sqlalchemy

from .io import manage_input, attach_signals
from .db import session_builder, ImportRegistry
from .cli import cli_parser

import logging
_logger = logging.getLogger(__name__)

# TODO: Move to config?
# TODO: Update measurements
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


def unsafe_preliminar_delete_import_range(session):
    to_delete = session.scalars(sqlalchemy.select(ImportRegistry).order_by(ImportRegistry.id))
    unsafe_delete_import_range(session, to_delete)
    session.execute(sqlalchemy.delete(ImportRegistry))

    _logger.info('COMPLETED DELETION PROCESS')


def unsafe_rollback_import(session, to_delete):
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

        # NOTE: It's faster to use short-circuit logic than separating this into a if-else construct.
        #       This is a safer way to extract the last record if exist one basically.
        current_import_range = ranges_inserted and ranges_inserted[-1]

        # Check whether these ranges are mergeable
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
        _logger.warning("Unable to insert the current batch")


def main():
    parser = cli_parser()
    args   = parser.parse_args()

    stream    = manage_input(args.input)
    delimiter = args.delimiter

    with stream as file, session_builder() as session:
        reader = csv.reader(file, delimiter = delimiter)

        # NOTE: We avoid using the DictReader because it will impact
        #       the performance negatively. In our case, it's better
        #       to manage the records as tuples or lists instead.

        # NOTE: We always assume that the csv is well formed AKA.
        #       it has a header and a consistent number of columns.
        header = next(reader)
        _logger.info(f'Retrieved Header: {header}')

        with session.begin():
            unsafe_preliminar_delete_import_range(session)

        # Collects the values currently inserted so we can apply a manual
        # rollback if the user cancels/close this process.
        ranges_inserted = [] 

        try:
            attach_signals()
            for i, chunk in enumerate(batched(reader, CHUNK_SIZE)):
                with session.begin():
                    _logger.info(f'Starting batch no. {i} (SENT: {i * CHUNK_SIZE} VALUES)')
                    unsafe_import_records(session, chunk, ranges_inserted)
            _logger.info('Import process complete!')
        except (InterruptedError, psycopg.Error):
            with session.begin():
                unsafe_rollback_import(session, ranges_inserted)
            raise
