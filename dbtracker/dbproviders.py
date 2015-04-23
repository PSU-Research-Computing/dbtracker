import pymysql
import psycopg2
import logging
import warnings
import datetime
import itertools
import sys
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Database(object):

    def __init__(self, host, user, password, port=None, engine=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = None
        self. engine = None

    def dictfetchall(self, cursor):
        "Returns all rows from a cursor as a dict"
        # From
        # https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
        desc = cursor.description
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()]

    def count_rows(self):
        raise NotImplementedError

    def connection(self):
        raise NotImplementedError

    def get_tables(self):
        raise NotImplementedError

    def db_rowcount(self, tables):
        dbs = {}
        for table in tables:
            if table["row_count"]:
                if table["db_name"] in dbs:
                    dbs[table["db_name"]] += table["db_name"]
                else:
                    dbs[table["db_name"]] = table["db_name"]
        return dbs


class Mysql(Database):

    def __init__(self, host, user, password, port=None, engine=None):
        super().__init__(host, user, password, port, engine)

    @contextmanager
    def connection(self):
        logger.info('Collecting mySQL stats')
        try:
            # mysql conext manager returns cursor
            with pymysql.connect(self.host, self.user, self.password) as curs:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    yield curs
        except pymysql.err.OperationalError as e:
            logger.error("mySQL error: %s" % e)

    def count_rows(self, cursor, tables):
        for table in tables:
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM `%(db)s`.`%(table)s`" %
                    {"db": table.get("TABLE_SCHEMA"),
                     "table": table.get("TABLE_NAME")})
                row_count = self.dictfetchall(cursor)[0]['COUNT(*)']
            except pymysql.err.InternalError as err:
                logger.info('Skipping: %s', "{}.{}".format(
                    table.get("TABLE_SCHEMA"), table.get("TABLE_NAME")),
                    extra={'err': err})
            finally:
                table['row_count'] = row_count or 0
        return tables

    def normalize(self, tables):
        normalized = []
        for table in tables:
            row = {}
            row["db_provider"] = "mysql"
            row["db_name"] = table["TABLE_SCHEMA"]
            row["table_name"] = table["TABLE_NAME"]
            row["schema_name"] = ""
            row["row_count"] = table["row_count"]
            normalized.append(row)
        return normalized

    def get_tables(self):
        with self.connection() as cursor:
            cursor.execute("SELECT * FROM information_schema.tables WHERE \
                TABLE_TYPE != 'VIEW'")
            tables = self.dictfetchall(cursor)
            tables = self.count_rows(cursor, tables)
            logger.info('mySQL stats collected')
            return self.normalize(tables)

    def db_rowcount(self):
        pass


class Postgres(Database):

    def __init__(self, host, user, password, port=None, engine=None):
        super().__init__(host, user, password, port, engine)

    @contextmanager
    def connection(self, database):
        try:
            with psycopg2.connect(host=self.host,
                                  user=self.user,
                                  password=self.password,
                                  database=database) as conn:
                with conn.cursor() as curs:
                    yield curs
        except psycopg2.DatabaseError as err:
            # Handle connection errors
            logger.info('Skipping: %s', database, extra={'err': err})
        except Exception as e:
            logger.error(e)

    def get_dbs(self):
        with self.connection(database='postgres') as cursor:
            cursor.execute("SELECT datname FROM pg_database WHERE \
                datistemplate = false")
            return self.dictfetchall(cursor)

    def count_rows(self, database):
        try:
            with self.connection(database=database) as cursor:
                cursor.execute("SELECT schemaname,relname,n_live_tup FROM \
                    pg_stat_user_tables ORDER BY n_live_tup DESC;")
                return self.dictfetchall(cursor)
        except RuntimeError:
            # Skip when there are connection errors
            return

    def normalize(self, tables):
        normalized = []
        for table in tables:
            row = {}
            row["db_provider"] = "pg"
            row["db_name"] = table["db_name"]
            row["table_name"] = table["relname"]
            row["schema_name"] = table["schemaname"]
            row["row_count"] = table["n_live_tup"]
            normalized.append(row)
        return normalized

    def get_tables(self):
        pg_dbs = self.get_dbs()
        pg_tables = []
        for db in pg_dbs:
            db_name = db.get("datname")
            pg_db_tables = self.count_rows(database=db_name)
            if pg_db_tables:
                for table in pg_db_tables:
                    table['db_name'] = db_name
                pg_tables = pg_tables + pg_db_tables
        return self.normalize(pg_tables)

    def db_rowcount(self):
        pass


class Storage(Postgres):

    def __init__(self, host, user, password, database, port=None, engine=None):
        super().__init__(host, user, password, port, engine)
        self.database = database

    def insert(self, cursor, date_time, db_provider, db_name, schema_name,
               table_name, row_count):
        cursor.execute("""INSERT INTO stats (datetime, db_provider, db_name, \
            schema_name, table_name, row_count) VALUES (%(date)s, \
            %(db_provider)s, %(dbname)s, %(schema)s, %(table)s, %(rows)s)""",
                       {'date': date_time, 'db_provider': db_provider,
                        'dbname': db_name, 'schema': schema_name,
                        'table': table_name, 'rows': row_count})

    def save(self, *dumps, timestamp=datetime.datetime.now()):
        tables = list(itertools.chain.from_iterable(dumps))
        with self.connection(self.database) as cursor:
            for table in tables:
                try:
                    self.insert(cursor=cursor,
                                date_time=timestamp,
                                db_provider=table["db_provider"],
                                db_name=table["db_name"],
                                schema_name=table["schema_name"],
                                table_name=table["table_name"],
                                row_count=table["row_count"])
                except psycopg2.DatabaseError as e:
                    # handle insert errors
                    print('Error %s' % e)
                    sys.exit(1)

    def get_history(self, number):
        with self.connection(self.database) as cursor:
            cursor.execute(
                "SELECT DISTINCT datetime FROM stats ORDER BY datetime DESC \
                LIMIT %(limit)s;", {'limit': number})
            return self.dictfetchall(cursor)

    def get_timestamp(self, timestamp, db_provider):
        with self.connection(self.database) as cursor:
            cursor.execute(
                "SELECT * FROM stats WHERE datetime = %(date)s AND \
                db_provider = %(db)s ORDER BY row_count;",
                {'date': timestamp, 'db': db_provider})
            return self.dictfetchall(cursor)
