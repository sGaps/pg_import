"""
    Entrypoint of the module. Let user invoke this module
    by writting in the console:
        $ python3 -m pg_import [PARAMS]

    INPUT
        A csv content read from a file or stdin. This content
        must be well formed and match the attributes of the
        model SaleOrder, which is defined in db.py.
    OUTPUT
        The records of the csv content will be inserted into
        a specified PostgreSQL instance.
"""
from . import main

main()