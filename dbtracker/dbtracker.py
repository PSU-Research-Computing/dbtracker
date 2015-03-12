import pymysql
import psycopg2
import datetime
import sys
from .local_settings import DATABASES

def cli(args):
    now = datetime.datetime.now()
    storage_con = postgresql('storage', 'dbtracker')
    save_mysql_stats('mysql', storage_con, now)
    save_pg_stats('postgresql', storage_con, now)
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

def save_mysql_stats(mysql_settings, scon, timestamp):
    con = mysql(mysql_settings)
    cursor = con.cursor()
    scursor = scon.cursor()
    cursor.execute("SELECT * FROM information_schema.tables")
    for table in dictfetchall(cursor):
        insert(
            scursor=scursor,
            date_time=timestamp,
            db_name=table.get('TABLE_SCHEMA'),
            table_name=table.get('TABLE_NAME'),
            row_count=table.get('TABLE_ROWS') or 0,
            )
    scon.commit()
    con.close()
    return

def save_pg_stats(pg_settings, scon, timestamp):
    con = postgresql(pg_settings, 'postgres')
    scursor = scon.cursor()
    pg_dbs = get_pg_dbs(con)
    con.close()
    for db in pg_dbs:
        db_name = db.get("datname")
        print(db_name)
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
                        schema_name=table.get('schema_name') or '',
                        row_count=table.get('n_live_tup')
                        )
                except psycopg2.DatabaseError as e:
                    print('Error %s' % e)
                    if con:
                        con.rollback()
                    sys.exit(1)
        except psycopg2.DatabaseError as e:
            print('Error %s' % e)
            if con:
                con.rollback()
            sys.exit(1)
        finally:
            scon.commit()
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

def insert(scursor, date_time="", db_name="", schema_name="", table_name="", row_count=0):
    scursor.execute(
        """INSERT INTO stats (datetime, db_name, schema_name, table_name, row_count) VALUES (%(date)s, %(dbname)s, %(schema)s, %(table)s, %(rows)s)""",
        {'date': date_time, 'dbname': db_name, 'schema': schema_name, 'table': table_name, 'rows': row_count})
    return date_time, db_name, schema_name, table_name, row_count

# From https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
