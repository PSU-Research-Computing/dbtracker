import unittest
from dbtracker import dbproviders
from dbtracker import configurator

# These tests require access to the production databases and a properly
# configured dbtracker
config = configurator.read_config()


class TestMysqlProvider(unittest.TestCase):

    def test_test(self):
        self.assertTrue(True, "The tests ran")

    def test_mysql_connection(self):
        mysql = dbproviders.Mysql(**config._sections['mysql'])
        self.assertIsNotNone(mysql)
        with mysql.connection() as cursor:
            self.assertIsNotNone(cursor)

    @unittest.skip("slow")
    def test_mysql_get_tables(self):
        mysql = dbproviders.Mysql(**config._sections['mysql'])
        print(mysql.get_tables())


class TestPostgressProvider(unittest.TestCase):

    def test_postgres_connection(self):
        pg = dbproviders.Postgres(**config._sections['postgresql'])
        with pg.connection("postgres") as cursor:
            self.assertIsNotNone(cursor)

    @unittest.skip("slow")
    def test_postgres_get_tables(self):
        pg = dbproviders.Postgres(**config._sections['postgresql'])
        pg_tables = pg.get_tables()
        self.assertIsNotNone(pg_tables)


class TestStorageProvider(unittest.TestCase):

    def test_arg_joining(self):
        storage = dbproviders.Storage(**config._sections['storage'])
        mysql = dbproviders.Mysql(**config._sections['mysql'])
        pg = dbproviders.Postgres(**config._sections['postgresql'])
        storage.save_db_dump(mysql.get_tables(), pg.get_tables())
