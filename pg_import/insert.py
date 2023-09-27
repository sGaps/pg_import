import csv
from itertools import islice

import psycopg
import sqlalchemy

from .io import manage_input, attach_signals
from .db import session_builder, ImportRegistry
from .cli import cli_parser

import logging
_logger = logging.getLogger(__name__)

# BENCHMARKS || [HARDWARE] CPU: i5-10400, RAM: 2x8 GiB @ 2400 MT/s, SWAP: 16GiB (HDD) || [ENVIRONMENT] OS: Manjaro 23.0 , Kernel: 5.15.131-1 | KDE Plasma
# ------------------------------------------------------------------------------------------------------------------------------------------------------------
# CHUNK_SIZE = 1_000_000    # TIME: 08m 53s ~ ???????, RAM: 583 MiB ~ 002 GiB. CPU <python>: 100%. CPU <system>: 20% (max), 10% (min). (<< TOO MANY RESOURCES)
# CHUNK_SIZE = 100_000      # TIME: 08m 33s ~ 08m 55s, RAM: 410 MiB ~ 680 MiB. CPU <python>: 100%. CPU <system>: 21% (max), 11% (min).
# CHUNK_SIZE = 10_000       # TIME: 08m 30s ~ 08m 53s, RAM: 093 MiB ~ 150 MiB. CPU <python>: 075%. CPU <system>: 17% (max), 11% (min). (<< OPTIMAL)
# CHUNK_SIZE = 1_000        # TIME: 10m 55s ~ 11m 03s, RAM: 053 MiB ~ 069 MiB. CPU <python>: 070%. CPU <system>: 17% (max), 11% (min).

# OPTIMAL CHUNK SIZE:
CHUNK_SIZE = 10_000

def batched(iterable, n):
    """
        Batch data into tuples of length n.
        The last batch may be shorter.
            batched('ABCDEFG', 3) --> ABC DEF G

        SEE ALSO: https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def unsafe_preliminar_delete_import_range(session):
    """
        Deletes the previously imported records and their
        corresponding import range registry from the db.
    """
    to_delete = session.scalars(sqlalchemy.select(ImportRegistry).order_by(ImportRegistry.id))
    unsafe_delete_import_range(session, to_delete)
    session.execute(sqlalchemy.delete(ImportRegistry))

    _logger.info('COMPLETED DELETION PROCESS')


def unsafe_rollback_import(session, to_delete):
    """
        Deletes the imported records added by this program
        and their corresponding import range registry.

        to_delete: [ImportRegistry]
            sequence of import ranges that marks which
            records must be deleted in SaleOrder. once
            the orders are deleted, each record inside
            to_delete is removed from the db as well.
    """
    unsafe_delete_import_range(session, to_delete)
    for record in to_delete:
        _logger.info(f"Deleting: {record.start_id} ... {record.end_id} (size: {record.end_id - record.start_id + 1} elements)")
        session.delete(record)
    _logger.info('COMPLETED DELETION PROCESS')
    _logger.warning('Remember to vacuum your database later')


def unsafe_delete_import_range(session, to_delete):
    """
        Deletes the imported records marked by each range
        inside to_delete.

        to_delete: [ImportRegistry]
            sequence of import ranges that marks which
            records must be deleted in SaleOrder.
    """
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


def unsafe_import_records(session, chunk, ranges_inserted):
    """
        Inserts a new chunk of records into the database
        and also register the chunk of ids into the import
        registry.

        ranges_inserted: [ImportRegistry]
            list of cached ImportRegistry instances used to determine if
            we must merge the current import range or if we should create
            a new import range.
            
        A new import range is created if the current and previous
        range are not mergeable. Two ranges are mergeable if their
        union describes a continuous sequence, for example:

            let a = [1, 10] and, b = [11, 23]
            both a and b are mergeable because if we mix them,
            the result will be: [1, 23]

            in the other hand, if c = [26, 30], then
            b and c are not mergeable because if we join their
            ranges, the result will be:
                [11, 23] <hole of size 2> [26, 30]
    """
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
    """
        main routine. Coordinates all the concepts and ideas into a single function.
    """
    parser = cli_parser()
    args   = parser.parse_args()

    stream    = manage_input(args.input)
    delimiter = args.delimiter

    has_header = args.has_header

    with stream as file, session_builder() as session:
        reader = csv.reader(file, delimiter = delimiter)

        # NOTE: We avoid using the DictReader because it will impact
        #       the performance negatively. In our case, it's better
        #       to manage the records as tuples or lists instead.

        # NOTE: We always assume that the csv is well formed AKA.
        #       it has a consistent number of columns.
        if has_header:
            header = next(reader, '<empty>')
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
