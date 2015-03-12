import logging
from .dbtracker import cli
import argparse


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Quiries MySQL and Postgres for stats")
    parser.add_argument("-S", "--save",
                        action="store_true", help="and save them to a database")
    args = parser.parse_args(argv)
    cli(args)
