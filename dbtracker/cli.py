import datetime
import logging
import sys
# from dbtracker.console_graph import print_bars
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
        elif args.date:
            self.date()

    def save(self):
        now = datetime.datetime.now()

        mysql_tables = self.mysql.get_tables()
        pg_tables = self.pg.get_tables()

        self.storage.save(mysql_tables, pg_tables, timestamp=now)

    def history(self):
        timestamps = self.storage.get_history(self.args.history)
        for i, timestamp in enumerate(timestamps):
            date = timestamp['datetime']
            print("{}: {} {}".format(i, date, date.strftime("%A")))

    def growth(self):
        runs = self.args.growth.split("-")
        if len(runs) == 1:
            t1, t2 = 1, runs[0]
        elif len(runs) == 2:
            t1, t2 = runs[0], runs[1]
        else:
            logger.warning("Cant parse range")
            sys.exit(1)
        diff = self.difference(int(t1), int(t2))
        print(diff)

    def count(self):
        raise NotImplementedError

    def date(self):
        raise NotImplementedError

    def difference(self, t1, t2):
        hrange = self.storage.get_history(max([t1, t2]))
        d1mysql = self.storage.get_timestamp(hrange[t1], 'mysql')
        d2mysql = self.storage.get_timestamp(hrange[t2 - 1], 'mysql')
        d1pg = self.storage.get_timestamp(hrange[t1], 'pg')
        d2pg = self.storage.get_timestamp(hrange[t2 - 1], 'pg')
        return d1pg
