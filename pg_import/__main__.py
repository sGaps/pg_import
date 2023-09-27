from . import main

# INPUT
#   A csv content read from a file or stdin. This content
#   must be well formed and match the attributes of the
#   model SaleOrder, which is defined in db.py.
# OUTPUT
#   The records of the csv content will be inserted into
#   a specified PostgreSQL instance.
main()