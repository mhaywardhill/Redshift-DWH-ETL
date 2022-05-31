import logging
import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(message)s"
)

def drop_tables(cur, conn):
    """ drop tables"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ create tables"""
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

    # drop tables	
    logging.info("Start: drop tables")
    drop_tables(cur, conn)
    logging.info("Finish: drop tables")

    # create tables
    logging.info("Start: create tables")	
    create_tables(cur, conn)
    logging.info("Finish: create tables")

    if conn:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
