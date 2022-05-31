import logging
import sys
import configparser
import psycopg2
from sql_queries import copy_table_queries
import os

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(message)s"
)


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_DB_PASSWORD = os.environ["DWH_DB_PASSWORD"]
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_HOST = os.environ["DWH_ENDPOINT"]

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT
        )
    )
    cur = conn.cursor()

    # load staging tables
    logging.info("Start: load staging tables")
    load_staging_tables(cur, conn)
    logging.info("Finish: load staging tables")

    if conn:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
