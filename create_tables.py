import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_DB 	= config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_PORT    = config.get("DWH","DWH_PORT")
    DWH_DB_PASSWORD = os.environ['DWH_DB_PASSWORD']
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_HOST = os.environ['DWH_ENDPOINT']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
