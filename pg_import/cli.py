import argparse

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
        help = 'indicates which character will be used as delimiter when reading the csv input',
    )
    parser.add_argument(
        '--no-header',
        action  = 'store_false',
        dest    = 'has_header',
        help    = "when specified, the program won't discard the first line of the file",
    )
    return parser
