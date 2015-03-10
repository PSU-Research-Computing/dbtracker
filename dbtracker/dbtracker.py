import pymysql
import psycopg2
import datetime
from .local_settings import DATABASES


def cli(args):
    return get_pg_dbs(postgres())

def mysql():
    conn = pymysql.connect(
        host=DATABASES.get('mysql').get('HOST'), 
        password=DATABASES.get('mysql').get('PASSWORD'), 
        user=DATABASES.get('mysql').get('USER')
        )
    return conn

def postgres():
    conn = psycopg2.connect(
        host=DATABASES.get('postgresql').get('HOST'), 
        password=DATABASES.get('postgresql').get('PASSWORD'), 
        user=DATABASES.get('postgresql').get('USER'),
        database="postgres"
        )
    return conn

def storage_db():
    conn = psycopg2.connect(
        host=DATABASES.get('storage').get('HOST'), 
        password=DATABASES.get('storage').get('PASSWORD'), 
        user=DATABASES.get('storage').get('USER'),
        database="dbtracker"
        )
    return conn

def get_pg_dbs(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
    for key in dictfetchall(cursor):
        print(key.get("datname"))
    return

def dump(conn):
    now = datetime.datetime.now()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM information_schema.tables")
    scon = storage_db()
    scursor = scon.cursor()
    for key in dictfetchall(cursor):
        insert(
            scursor=scursor,
            date_time=now,
            db_name=key.get('TABLE_SCHEMA'),
            table_name=key.get('TABLE_NAME'),
            row_count=key.get('TABLE_ROWS') or 0,
            )
    scon.commit()
    return

def insert(scursor,date_time="", db_name="", schema_name="", table_name="", row_count=0):
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
