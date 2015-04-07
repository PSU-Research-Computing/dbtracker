import logging
from .dbtracker import cli
import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Queries MySQL and PostgreSQL for stats.")
    parser.add_argument("-S", "--save",
                        action="store_true", help="generate and save database stats")
    parser.add_argument("-g", "--growth",
                        help="show the growth from the last nth timestamp", type=int)
    parser.add_argument(
        "-H", "--history", help="List the times of the last n saved runs", type=int)
    parser.add_argument("-c", "--count", action="store_true",
                        help="Gets database row counts but does not save")
    parser.add_argument("-r", "--range", type=str,
                        help="compares history range eg 3-6")
    parser.add_argument("-d", "--date", type=str,
                        help="compares date to last run")
    parser.add_argument("-D", "--date-range", type=str,
                        help="compares stats between two dates")
    parser.add_argument("-s", "--silent", action="store_true",
                        help="turns logging levels down to ")
    args = parser.parse_args(argv)
    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.INFO)
    cli(args)
