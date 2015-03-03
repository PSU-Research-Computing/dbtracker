import pymysql
import psycopg2
from local_settings import DATABASES


def cli(args):
    return dump(mysql())

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
        database="postgres"
        )
    return conn

def dump(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM information_schema.tables")
    print(cursor.fetchall())
    return cursor.fetchall()

# From https://github.com/PSU-OIT-ARC/django-arcutils/blob/master/arcutils/__init__.py#L82
def dictfetchall(cursor):
    "Returns all rows from a cursor as a dict"
    desc = cursor.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cursor.fetchall()
    ]
