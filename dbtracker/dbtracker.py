import pymysql
import psycopg2
import datetime
import sys
import logging
import pprint
import warnings
from .local_settings import DATABASES

logger = logging.getLogger(__name__)

def cli(args):
    now = datetime.datetime.now()
    if args.save:
        storage_con = postgresql('storage', 'dbtracker')
        logger.info('Connected to storage db')
        mysql_stats('mysql', storage_con, now)
        pg_stats('postgresql', storage_con, now)
        storage_con.commit()
        logger.info('Stats commited to strage db!')
    else:
        db_row_count = mysql_stats('mysql').copy()
        db_row_count.update(pg_stats('postgresql'))
        pprint.pprint(db_row_count, width=1)
    return

def mysql(mysql_settings):
    conn = pymysql.connect(
        host=DATABASES.get(mysql_settings).get('HOST'),
        password=DATABASES.get(mysql_settings).get('PASSWORD'),
        user=DATABASES.get(mysql_settings).get('USER')
        )
    return conn

def postgresql(pg_settings, db_name):
    conn = psycopg2.connect(
        host=DATABASES.get(pg_settings).get('HOST'),
        password=DATABASES.get(pg_settings).get('PASSWORD'),
        user=DATABASES.get(pg_settings).get('USER'),
        database=db_name
        )
    return conn

def mysql_stats(mysql_settings, scon=None, timestamp=None):
    dbs = None
    logger.info('Collecting mysql stats...')
    con = mysql(mysql_settings)
    cursor = con.cursor()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cursor.execute("SELECT * FROM information_schema.tables")
    tables = dictfetchall(cursor)
    if scon:
        save_mysql_stats(scon, tables, timestamp)
    else:
        dbs = count_mysql_stats(tables)
    con.close()
    logger.info('Mysql stats complete.')
    return dbs

def save_mysql_stats(scon, tables, timestamp):
    scursor = scon.cursor()
    for table in tables:
        insert(
            scursor=scursor,
            date_time=timestamp,
            schema_name="",
            db_name=table.get('TABLE_SCHEMA'),
            table_name=table.get('TABLE_NAME'),
            row_count=table.get('TABLE_ROWS') or 0,
            )
    return

def count_mysql_stats(tables):
    dbs = {}
    for table in tables:
        if table['TABLE_ROWS']:
            if table['TABLE_SCHEMA'] in dbs:
                dbs[table['TABLE_SCHEMA']] += table['TABLE_ROWS']
            else:
                dbs[table['TABLE_SCHEMA']] = table['TABLE_ROWS']
    return dbs

def pg_stats(pg_settings, scon=None, timestamp=None):
    dbs = None
    logger.info('Collecting postgresql stats...')
    con = postgresql(pg_settings, 'postgres')
    pg_dbs = get_pg_dbs(con)
    con.close()
    if scon:
        save_pg_stats(pg_settings, scon, pg_dbs, timestamp)
    else:
        dbs = count_pg_stats(pg_settings, pg_dbs)
    logger.info('Postgresql stats complete')
    return dbs

def count_pg_stats(pg_settings, pg_dbs):
    dbs = {}
    for db in pg_dbs:
        db_name = db.get("datname")
        try:
            con = postgresql(pg_settings, db_name)
            cursor = con.cursor()
            cursor.execute("SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;")
            for table in dictfetchall(cursor):
                if table['n_live_tup']:
                    if db_name in dbs:
                        dbs[db_name] += table['n_live_tup']
                    else:
                        dbs[db_name] = table['n_live_tup']
        except psycopg2.DatabaseError as e:
            logger.warning('Skipping: %s', db_name, extra={'e': e})
    return dbs

def save_pg_stats(pg_settings, scon, pg_dbs, timestamp):
    scursor = scon.cursor()
    for db in pg_dbs:
        db_name = db.get("datname")
        try:
            con = postgresql(pg_settings, db_name)
            cursor = con.cursor()
            cursor.execute("SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;")
            for table in dictfetchall(cursor):
                try:
                    insert(
                        scursor=scursor,
                        date_time=timestamp,
                        db_name=db_name,
                        table_name=table.get('relname'),
                        schema_name=table.get('schemaname'),
                        row_count=table.get('n_live_tup')
                        )
                except psycopg2.DatabaseError as e:
                    # handle insert errors
                    print('Error %s' % e)
                    sys.exit(1)
        except psycopg2.DatabaseError as e:
            # Handle connection errors
            logger.warning('Skipping: %s', db_name, extra={'e': e})
        finally:
            # Commit and close if all went well
            con.close()
    return

def get_pg_dbs(pg_con):
    cursor = pg_con.cursor()
    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
    pg_dbs = dictfetchall(cursor)
    return pg_dbs

def get_info_schema(con):
    cursor = con.cursor()
    cursor.execute("SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;")
    for key in dictfetchall(cursor):
        print(key)
    return

def insert(scursor, date_time, db_name, schema_name, table_name, row_count):
    scursor.execute(
        """INSERT INTO stats (datetime, db_name, schema_name, table_name, row_count) VALUES (%(date)s, %(dbname)s, %(schema)s, %(table)s, %(rows)s)""",
        {'date': date_time, 'dbname': db_name, 'schema': schema_name, 'table': table_name, 'rows': row_count})
    return date_time, db_name, schema_name, table_name, row_count

def debug(scursor, date_time="", db_name="", schema_name="", table_name="", row_count=0):
    """insert to terminal instead of a database"""
    print(date_time, db_name, table_name, schema_name or '', row_count)
    return

# From https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
