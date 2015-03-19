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
    mysql_tables = get_mysql_tables('mysql')
    pg_tables = get_pg_tables('postgresql')
    if args.save:
        try:
            storage_con = postgresql_con('storage', 'dbtracker')
            logger.info('Connected to storage db')
            save_mysql_stats(storage_con, mysql_tables, now)
            #save_pg_stats(storage_con, pg_tables, now)
            storage_con.commit()
            logger.info('Stats commited to strage db!')
        except psycopg2.DatabaseError as err:
            logger.warning(
                "Can't connect to storage db: %s", err, extra={'e': err})
    else:
        print('hi')
    #    mysql_stats('mysql', storage_con, now)
    #    pg_stats('postgresql', storage_con, now)
    #    storage_con.commit()
    #    logger.info('Stats commited to strage db!')
    # else:
    #    db_row_count = mysql_stats('mysql').copy()
    #    db_row_count.update(pg_stats('postgresql'))
    #    pprint.pprint(db_row_count, width=1)
    # return


# Mysql Functions
def mysql_con(mysql_settings):
    conn = pymysql.connect(
        host=DATABASES.get(mysql_settings).get('HOST'),
        password=DATABASES.get(mysql_settings).get('PASSWORD'),
        user=DATABASES.get(mysql_settings).get('USER')
    )
    return conn


def get_mysql_tables(mysql_settings):
    logger.info('Collecting mysql stats...')
    con = mysql_con(mysql_settings)
    cursor = con.cursor()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cursor.execute(
            "SELECT * FROM information_schema.tables WHERE TABLE_TYPE != 'VIEW'")
    tables = dictfetchall(cursor)
    con.close()
    logger.info('Mysql stats collected.')
    return tables


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


# Postgresql Functions
def postgresql_con(pg_settings, db_name):
    conn = psycopg2.connect(
        host=DATABASES.get(pg_settings).get('HOST'),
        password=DATABASES.get(pg_settings).get('PASSWORD'),
        user=DATABASES.get(pg_settings).get('USER'),
        database=db_name
    )
    return conn


def get_pg_dbs(pg_settings):
    pg_con = postgresql_con(pg_settings, 'postgres')
    cursor = pg_con.cursor()
    cursor.execute(
        "SELECT datname FROM pg_database WHERE datistemplate = false")
    pg_dbs = dictfetchall(cursor)
    pg_con.close()
    return pg_dbs


def get_pg_tables(pg_settings):
    logger.info('Collecting postgresql stats...')
    pg_dbs = get_pg_dbs(pg_settings)
    logger.info('Retreived postgresql databases...')
    pg_tables = []
    for db in pg_dbs:
        db_name = db.get("datname")
        pg_db_tables = get_pg_db_tables(pg_settings, db_name)
        if pg_db_tables:
            for table in pg_db_tables:
                table['db_name'] = db_name
            pg_tables = pg_tables + pg_db_tables
    logger.info('Postgresql stats collected.')
    return pg_tables


def get_pg_db_tables(pg_settings, db_name):
    try:
        con = postgresql_con(pg_settings, db_name)
        cursor = con.cursor()
        cursor.execute(
            "SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;")
        pg_db_stats = dictfetchall(cursor)
    except psycopg2.DatabaseError as e:
        # Handle connection errors
        logger.warning('Skipping: %s', db_name, extra={'e': e})
        return None
    else:
        con.close()
        return pg_db_stats


def save_pg_stats(scon, pg_tables, timestamp):
    scursor = scon.cursor()
    for table in pg_tables:
        try:
            insert(scursor=scursor,
                   date_time=timestamp,
                   db_name=table.get('db_name'),
                   table_name=table.get('relname'),
                   schema_name=table.get('schemaname'),
                   row_count=table.get('n_live_tup'))
        except psycopg2.DatabaseError as e:
            # handle insert errors
            print('Error %s' % e)
            sys.exit(1)


def count_pg_stats(pg_settings, pg_dbs):
    dbs = {}
    for db in pg_dbs:
        db_name = db.get("datname")
        try:
            con = postgresql(pg_settings, db_name)
            cursor = con.cursor()
            cursor.execute(
                "SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;")
            for table in dictfetchall(cursor):
                if table['n_live_tup']:
                    if db_name in dbs:
                        dbs[db_name] += table['n_live_tup']
                    else:
                        dbs[db_name] = table['n_live_tup']
        except psycopg2.DatabaseError as e:
            logger.warning('Skipping: %s', db_name, extra={'e': e})
    return dbs


# Util
def insert(scursor, date_time, db_name, schema_name, table_name, row_count):
    scursor.execute(
        """INSERT INTO stats (datetime, db_name, schema_name, table_name, row_count) VALUES (%(date)s, %(dbname)s, %(schema)s, %(table)s, %(rows)s)""",
        {'date': date_time, 'dbname': db_name, 'schema': schema_name, 'table': table_name, 'rows': row_count})
    return date_time, db_name, schema_name, table_name, row_count


def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    # From
    # https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
