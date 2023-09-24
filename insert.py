

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
def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


from db import session_builder, SaleOrder
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
CHUNK_SIZE = 10_000
with stream as file, session_builder() as session:
    # reader = csv.reader(file, delimiter = delimiter)
    reader = csv.DictReader(file, delimiter = delimiter)

    # NOTE: for now, we assume that we always have a header inside the csv files
    # NOTE: We can use DictReader, but it assumes that the header always have
    #       well written names, but in our case, the first column name has an invisible
    #       utf-8 character that would lead to undefined behavior, so we must clean
    #       the names before translating these to 
    # NOTE: We could use DictReader, but it will imply a
    print('HEADER', reader.fieldnames)

    # TODO: REMOVE PREVIOUSLY INSERTED RECORDS.

    records_inserted = []

    SaleOrderTable = SaleOrder.__table__

    import time

    try:
        # for i, chunk in enumerate(batched(reader, CHUNK_SIZE)):
        for i, chunk in enumerate( batched(reader, CHUNK_SIZE) ):
            with session.begin():
                print('Starting batch no.', i, '(SENT:', i * CHUNK_SIZE, 'VALUES)')

                statement = (SaleOrderTable.insert()
                                .values(chunk)
                                .returning(SaleOrderTable.c.id))


                result = session.execute(statement)
                row    = result.first()
                if row:
                    start_id = row[0]
                    end_id   = start_id + CHUNK_SIZE - 1
                    records_inserted.append( (start_id, end_id) )
                else:
                    print("Records couldn't be inserted on batch no.", i)
    # TODO: Use atexit module?
    except Exception as err:
        print('Rolling back steps')
        for start_id, end_id in records_inserted:
            # Discard changes made on our process
            with session.begin():
                print('Deleting:', start_id, '...', end_id)
                # Removing values directly
                statement = SaleOrderTable.delete().where(
                    SaleOrderTable.c.id >= start_id,
                    SaleOrderTable.c.id <= end_id,
                )
                result = session.execute(statement)
        raise
