import logging
from dbtracker.cli import Cli
import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Queries MySQL and PostgreSQL for stats")
    parser.add_argument(
        "-S", "--save",
        action="store_true",
        help="generate and save database stats")
    parser.add_argument(
        "-g", "--growth",
        help="display a graph of the growth.  Arguments in the form of run number ranges e.g. 3-4 or 4",
        type=str)
    parser.add_argument(
        "-H", "--history",
        help="List the datetime stamps of the last n saved runs",
        type=int)
    parser.add_argument(
        "-c", "--count",
        action="store_true",
        help="Gets database row counts but does not save")
    parser.add_argument(
        "-d", "--dates",
        type=str,
        help="compares two datetime stamps e.g. 2015-04-24 16:18:57.166095-07:00 - 2015-04-22 17:00:50.746688-07:00")
    parser.add_argument(
        "-s", "--silent",
        action="store_true",
        help="turns logging levels down to ERROR only")
    parser.add_argument(
        "-C", "--config",
        type=str,
        help="use a custom configuration file path")

    args = parser.parse_args(argv)

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(level=logging.INFO)
    cli = Cli(args)
    cli.main()
