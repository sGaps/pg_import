"""
    pg_import
    ---------

    Module that let users import records without using the copy directive
    of PostgresSQL.


    Use the command:
        $ python3 -m pg_import [PARAMS]
    
    to run the main program of this module.
"""
from .insert import main
