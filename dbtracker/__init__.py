import logging
from .dbtracker import cli
import argparse


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Queries MySQL and PostgreSQL for stats.")
    parser.add_argument("-S", "--save",
                        action="store_true", help="generate and save database stats")
    parser.add_argument("-g", "--growth",
                        help="show the growth from the last nth timestamp", type=int)
    parser.add_argument(
        "-H", "--history", help="List the times of the last n saved runs", type=int)
    args = parser.parse_args(argv)
    cli(args)
