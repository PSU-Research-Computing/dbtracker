import datetime
import logging
import sys
from dateutil.parser import parse
from dbtracker.configurator import read_config
from dbtracker.dbproviders import Storage, Mysql, Postgres
from dbtracker.console_graph import print_bars

logger = logging.getLogger(__name__)


class Cli(object):

    def __init__(self, args):
        self.args = args
        try:
            config = read_config(
                file=args.config) if args.config else read_config()
            self.storage = Storage(**config._sections['storage'])
            self.mysql = Mysql(**config._sections['mysql'])
            self.pg = Postgres(**config._sections['postgresql'])
        except KeyError:
            logger.error("Invalid configuration")
            sys.exit(1)

    def main(self):
        args = self.args
        if args.save:
            self.save()
        elif args.history:
            self.history()
        elif args.growth:
            self.growth()
        elif args.count:
            self.count()
        elif args.dates:
            self.dates()
        else:
            print("Please pass -h for help")

    def save(self):
        now = datetime.datetime.now()

        mysql_tables = self.mysql.get_tables()
        pg_tables = self.pg.get_tables()

        self.storage.save(mysql_tables, pg_tables, timestamp=now)

    def history(self):
        timestamps = self.storage.get_history(self.args.history)
        for i, timestamp in enumerate(timestamps):
            date = timestamp['datetime']
            print("{}: {} [{}]".format(i, date, date.strftime("%A")))

    def growth(self):
        runs = self.args.growth.split("-")
        if len(runs) == 1:
            r1 = 0
            r2 = int(runs[0])
        elif len(runs) == 2:
            r1 = int(runs[0])
            r2 = int(runs[1])
        else:
            logger.warning("Cant parse range")
            sys.exit(1)
        d1, d2 = self.get_datetime_from_run(r1, r2)
        mysql_diff, pg_diff = self.run_difference(d1, d2)
        self.diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)

    def diff_printer(self, d1, d2, mysql=None, pg=None):
        print("==== PostgreSQL [{}] - [{}] ====".format(d1, d2))
        print_bars(pg)
        print("==== MySQL [{}] - [{}] ====".format(d1, d2))
        print_bars(mysql)

    def count_printer(self, d, mysql=None, pg=None):
        print("==== PostgreSQL [{}] ====".format(d))
        print_bars(pg)
        print("==== MySQL [{}] ====".format(d))
        print_bars(mysql)

    def dates(self):
        dates = self.args.dates.split(' - ')
        if len(dates) == 2:
            d1 = parse(dates[0], fuzzy=True)
            d2 = parse(dates[1], fuzzy=True)
            mysql_diff, pg_diff = self.run_difference(d1, d2)
            self.diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)
        else:
            logger.warning("Cant parse range")
            sys.exit(1)

    def get_datetime_from_run(self, r1, r2):
        hrange = self.storage.get_history(max([r1, r2]) + 1)
        d1 = hrange[r1]['datetime']
        d2 = hrange[r2]['datetime']
        return d1, d2

    def run_difference(self, d1, d2):
        mysql_diff = self.difference(d1, d2, 'mysql')
        pg_diff = self.difference(d1, d2, 'pg')
        return mysql_diff, pg_diff

    def difference(self, d1, d2, provider):
        d1_tables = self.storage.get_timestamp(d1, provider)
        d2_tables = self.storage.get_timestamp(d2, provider)

        d1_totals = self.storage.db_rowcount(d1_tables)
        d2_totals = self.storage.db_rowcount(d2_tables)

        diff = {}
        for key in d1_totals:
            diff[key] = d1_totals[key] - d2_totals.get(key, 0)
        return diff

    def count(self):
        now = datetime.datetime.now()
        mysql_tables = self.mysql.get_tables()
        pg_tables = self.pg.get_tables()

        mysql_totals = self.storage.db_rowcount(mysql_tables)
        pg_totals = self.storage.db_rowcount(pg_tables)

        self.count_printer(now, mysql=mysql_totals, pg=pg_totals)
