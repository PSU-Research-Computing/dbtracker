import pymysql
import psycopg2
import logging


logger = logging.getLogger(__name__)


class Database(object):

    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def get_tables(self):
        raise NotImplementedError

    def dictfetchall(self, cursor):
        "Returns all rows from a cursor as a dict"
        # From
        # https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
        desc = cursor.description
        return [
            dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()
        ]

    def count_rows(self):
        pass

    def connection(self):
        raise NotImplementedError


class Mysql(Database):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

    def get_tables(self):
        pass

    def connection(self):
        try:
            self._conn = pymysql.connect(
                host=self.host,
                password=self.password,
                user=self.user)
        except pymysql.err.OperationalError:
            logger.error("Error connecting to mySQL")


class Postgres(Database):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

    def get_tables(self):
        pass

    def connection(self, database):
        try:
            self._conn = psycopg2.connect(
                host=self.host,
                password=self.password,
                user=self.user,
                database=database)
        except:
            logger.error("Error connecting to postgreSQL")


class Storage(Postgres):

    def __init__(self, host, user, password):
        super().__init__(host, user, password)

    def insert(self):
        pass

    def save_db_dump(self):
        pass

    def get_history(self):
        pass

    def get_timestamp(self):
        pass
