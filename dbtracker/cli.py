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
        config = read_config(
            file=args.config) if args.config else read_config()
        self.storage = Storage(**config._sections['storage'])
        self.mysql = Mysql(**config._sections['mysql'])
        self.pg = Postgres(**config._sections['postgresql'])

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
            t1 = 0
            t2 = int(runs[0])
        elif len(runs) == 2:
            t1 = int(runs[0])
            t2 = int(runs[1])
        else:
            logger.warning("Cant parse range")
            sys.exit(1)
        d1, d2 = self.get_datetime_from_run(t1, t2)
        mysql_diff, pg_diff = self.run_difference(d1, d2)
        self.diff_printer(d1, d2, mysql=mysql_diff, pg=pg_diff)

    def diff_printer(self, d1, d2, mysql=None, pg=None):
        print("==== PostgreSQL {} - {} ====".format(d1, d2))
        print_bars(pg)
        print("==== MySQL [{}] - [{}] ====".format(d1, d2))
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

    def get_datetime_from_run(self, t1, t2):
        hrange = self.storage.get_history(max([t1, t2]) + 1)
        d1 = hrange[t1]['datetime']
        d2 = hrange[t2]['datetime']
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
