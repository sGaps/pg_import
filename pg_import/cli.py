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
        help = 'indicates which column delimiter will be used to read the csv input',
    )
    return parser
