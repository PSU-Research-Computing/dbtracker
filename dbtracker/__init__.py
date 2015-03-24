import logging
from .dbtracker import cli
import argparse


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Quiries MySQL and Postgres for stats")
    parser.add_argument("-S", "--save",
                        action="store_true", help="and save them to a database")
    parser.add_argument("-g", "--growth", action="store_true",
                        help="show the growth from the last nth timestamp")
    args = parser.parse_args(argv)
    cli(args)
