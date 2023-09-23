

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


parser = cli_parser()
args   = parser.parse_args()

stream    = manage_input(args.input)
delimiter = args.delimiter
with stream as file:
    reader = csv.reader(file, delimiter = delimiter)

    # NOTE: for now, we assume that we always have a header inside the csv files
    # NOTE: We can use DictReader, but it assumes that the header always have
    #       well written names, but in our case, the first column name has an invisible
    #       utf-8 character that would lead to undefined behavior, so we must clean
    #       the names before translating these to 
    # NOTE: We could use DictReader, but it will imply a
    header = next(reader)
    print('HEADER', header)

    # TODO: IMPROVE SOLUTION AND REMOVE ITERTOOLS
    for line in itertools.islice(reader, 10):
        print('GOT', line)
