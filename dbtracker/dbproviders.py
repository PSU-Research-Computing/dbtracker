import pymysql
import psycopg2
import logging
import warnings
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Database(object):

    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def dictfetchall(self, cursor):
        "Returns all rows from a cursor as a dict"
        # From
        # https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
        desc = cursor.description
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()]

    def count_rows(self):
        pass

    def connection(self):
        raise NotImplementedError

    def get_tables(self):
        raise NotImplementedError

    def query_for_tables(self, cursor):
        raise NotImplementedError


class Mysql(Database):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

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

    def get_tables(self):
        with self.connection() as cursor:
            cursor.execute("SELECT * FROM information_schema.tables WHERE \
                TABLE_TYPE != 'VIEW'")
            tables = self.dictfetchall(cursor)
            tables = self.count_rows(cursor, tables)
            logger.info('mySQL stats collected')
            return tables

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


class Postgres(Database):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

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
            return

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
        return pg_tables


class Storage(Postgres):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

    def insert(self, date_time, db_provider, db_name, schema_name, table_name, row_count):
        pass

    def save_db_dump(self):
        pass

    def get_history(self):
        pass

    def get_timestamp(self):
        pass
