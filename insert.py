

# INPUT
#   a csv file or stdin (they only need csv file really).
#       This file has rows that must be inserted into a local
#       PostgreSQL database

# take the rows and insert it into the database.
# NOTE: you must remove the content from a possible previous import.


# Delivery method:
#   Put the code in a public Github Repository.


# OS INTEGRATION
import argparse
import sys
import os
import io

from contextlib import contextmanager


@contextmanager
def manage_input(path = None):
    # TODO: Rewrite this note
    # NOTE: We may encounter files that have an UTF-8 signature
    #       which cannot be treated as the content file. So, we
    #       must specify that we will ignore the BOM by passing
    #       the following encoding. This also applies if we
    #       pass a file through stdin.
    if path and os.path.exists(path) and os.path.isfile(path):
        with open(path, 'r', encoding = 'utf-8-sig') as file:
            yield file
    else:
        istream = io.TextIOWrapper(sys.stdin.buffer, encoding = 'utf-8-sig')
        yield istream

def cli_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--input',
        metavar = '<file>',
        dest = 'input',
        help = 'Indicates which file will be read by this command. stdin used when not provided',
    )
    parser.add_argument(
        '-d',
        '--delimiter',
        metavar = '<char>',
        dest    = 'delimiter',
        default = ';',
        help = 'indicates which column delimiter will be used to read the csv input',
    )
    return parser

# REQUIRED
import csv
import psycopg
import sqlalchemy


# TODO: Consider to remove or use later
import itertools
# SEE ALSO: https://docs.python.org/3/library/itertools.html#itertools-recipes
# CONSIDER USE THIS INSTEAD: https://stackoverflow.com/questions/434287/how-to-iterate-over-a-list-in-chunks
def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


from db import session_builder, SaleOrder, ImportRegistry
from datetime import date
cast_date = date.fromisoformat

parser = cli_parser()
args   = parser.parse_args()

stream    = manage_input(args.input)
delimiter = args.delimiter


# TODO: DELETE LATER
import signal
def signal_handler(sig, frame):
    sig_obj = signal.Signals(sig)
    raise InterruptedError(f'rolling back changes due the presence of the signal: {sig_obj.name} ({sig})')
for s in (signal.SIGABRT, signal.SIGILL, signal.SIGINT, signal.SIGSEGV, signal.SIGTERM):
    signal.signal(s, signal_handler)


#CHUNK_SIZE = 655_350
# CHUNK_SIZE = 1_000_000 # TIME: 12m 27s, RAM: 3   GiB ~ 4GiB
# CHUNK_SIZE = 100_000   # TIME: 12m 14s, RAM: 410 MiB ~ 450MiB
# CHUNK_SIZE = 1_000     # TIME: 13n 05s, RAM: 53  MiB ~ 53 MiB
CHUNK_SIZE = 10_000      # TIME: 12m 06s, RAM: 93  MiB ~ 93MiB (OPTIMAL)
# CHUNK_SIZE = 5      # TIME: 12m 06s, RAM: 93  MiB ~ 93MiB
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
    print('HEADER', header)
    # print('HEADER', reader.fieldnames)


    records_inserted = []

    SaleOrderTable = SaleOrder.__table__
    ImportRegistryTable = ImportRegistry.__table__

    #TODO VERIFY LATER
    # with session.begin():
    #     cursor = session.connection().connection.cursor()
    #     delete_query = """
    #         -- | Remove the previous imports
    #         DELETE FROM sale_order so_del
    #             WHERE so_del.id IN (
    #                 SELECT
    #                     so.id
    #                 FROM
    #                     sale_order so
    #                     JOIN import_registry ir
    #                         ON (so.id >= ir.start_id AND so.id <= ir.end_id)
    #                 GROUP BY so.id);

    #         -- | Cleans the import registry
    #         DELETE FROM import_registry;
    #         """
    #     cursor.execute(delete_query)

    with session.begin():
        to_delete = session.scalars(sqlalchemy.select(ImportRegistry).order_by(ImportRegistry.id))
        cursor    = psycopg.ClientCursor(session.connection().connection)
        for i, chunk_to_delete in enumerate( batched(to_delete, CHUNK_SIZE) ):
            print('Deleting batch no.', i, '(DELETED:', i * CHUNK_SIZE, 'VALUES)')

            delete_query = "DELETE FROM sale_order WHERE "

            delete_template = 'id >= %s AND id <= %s\n'

            delete_query += ' OR '.join(
                    cursor.mogrify(delete_template, [record.start_id, record.end_id])
                    for record
                    in chunk_to_delete)

            cursor.execute(
                delete_query,
            )

        # PRUNE table
        # TODO: Consider trucate to deallocate the space to the OS:
        session.execute(sqlalchemy.delete(ImportRegistry))

        print('Delted a lot of records')



    # # TODO: IMPROVE. TOO SLOW
    # with session.begin():
    #     to_delete = session.scalars(sqlalchemy.select(ImportRegistry).order_by(ImportRegistry.id))
    #     deletion_ranges = []
    #     for record in to_delete:
    #         deletion_ranges.append(
    #             sqlalchemy.and_(
    #                 SaleOrder.id >= record.start_id,
    #                 SaleOrder.id <= record.end_id
    #             )
    #         )
    #     statement = sqlalchemy.delete(SaleOrder).where(
    #         sqlalchemy.or_(
    #             sqlalchemy.false(),
    #             *deletion_ranges
    #         )
    #     )

    #     print('Deleting records')
    #     result = session.execute(statement)
    #     print('Deleted a lot of records on SaleOrder', result.rowcount)
    #     # TODO: Delete all records inside ImportRegistry


    try:
        # TODO:Enable signals in this try/except block:

        current_import_range = None
        ranges = [] 
        for i, chunk in enumerate(batched(reader, CHUNK_SIZE)):
            with session.begin():
                print('Starting batch no.', i, '(SENT:', i * CHUNK_SIZE, 'VALUES)')
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
                        ranges.append(current_import_range)

                    # TODO: DELETE THIS SECTION BECAUSE IT IS DEPRECATED:
                    # merge results
                    if records_inserted and (start_id - 1 == records_inserted[-1][1]):
                        # extend range:
                        records_inserted[-1][1] = end_id
                    else:
                        records_inserted.append( [start_id, end_id] )
                else:
                    print("Records couldn't be inserted on batch no.", i)

    except (InterruptedError, psycopg.Error) as err:
        print('Rolling back steps')
        for start_id, end_id in records_inserted:
            # Discard changes made on our process
            with session.begin():
                print('Deleting:', start_id, '...', end_id, '(size: ', end_id - start_id + 1, 'elements)')
                # Removing values directly
                statement = SaleOrderTable.delete().where(
                    SaleOrderTable.c.id >= start_id,
                    SaleOrderTable.c.id <= end_id,
                )
                result = session.execute(statement)
        print('Remember to vacuum your database later')
        raise

    print(current_import_range)