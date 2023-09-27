"""
    The only purpose of this tiny file is to call
    the local module pg_import easily.

    You can call the module with this command:

        $ python3 run_import.py [PARAMS]

    instead of:
        $ python3 -m pg_import [PARAMS]
"""
import pg_import

pg_import.main()