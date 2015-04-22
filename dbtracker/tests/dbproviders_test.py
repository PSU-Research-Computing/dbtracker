import unittest
from dbtracker import dbproviders
from dbtracker import configurator

# These tests require access to the production databases and a properly
# configured dbtracker
config = configurator.read_config()

mysql_login = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
}

pg_login = {
    "host": config['postgresql']['host'],
    "user": config['postgresql']['user'],
    "password": config['postgresql']['password'],
}

storage_login = {
    "host": config['storage']['host'],
    "user": config['storage']['user'],
    "password": config['storage']['password'],
    "database": config['storage']['database'],
}


class TestMysqlProvider(unittest.TestCase):

    def test_test(self):
        self.assertTrue(True, "The tests ran")

    def test_mysql_connection(self):
        mysql = dbproviders.Mysql(**mysql_login)
        self.assertIsNotNone(mysql)
        with mysql.connection() as cursor:
            self.assertIsNotNone(cursor)

    @unittest.skip("slow")
    def test_mysql_get_tables(self):
        mysql = dbproviders.Mysql(**mysql_login)
        print(mysql.get_tables())


class TestPostgressProvider(unittest.TestCase):

    def test_postgres_connection(self):
        pg = dbproviders.Postgres(**pg_login)
        with pg.connection("postgres") as cursor:
            self.assertIsNotNone(cursor)

    @unittest.skip("slow")
    def test_postgres_get_tables(self):
        pg = dbproviders.Postgres(**pg_login)
        pg_tables = pg.get_tables()
        self.assertIsNotNone(pg_tables)


class TestStorageProvider(unittest.TestCase):

    def test_arg_joining(self):
        storage = dbproviders.Storage(**storage_login)
        mysql = dbproviders.Mysql(**mysql_login)
        pg = dbproviders.Postgres(**pg_login)
        storage.save_db_dump(mysql.get_tables(), pg.get_tables())
