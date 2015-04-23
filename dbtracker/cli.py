import datetime
import logging
# from dbtracker.console_graph import print_bars
from dbtracker.configurator import read_config
from dbtracker.dbproviders import Storage, Mysql, Postgres

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
            print("{}: {} {}".format(i + 1, date, date.strftime("%A")))

    def growth(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def date(self):
        raise NotImplementedError
