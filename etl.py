import configparser
import psycopg2
from sql_queries import copy_table_queries,\
    insert_table_queries, \
    truncate_table_queries


def load_staging_tables(cur, conn):
    """Load s3 data into tables

    Args:
        - cur (psycopg2 cursor)
        - conn
    Returns:
        None
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into tables

    Args:
        - cur (psycopg2 cursor)
        - conn
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def truncate_tables(cur, conn):
    """Truncate tables

    Args:
        - cur (psycopg2 cursor)
        - conn
    Returns:
        None
    """
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    # truncate_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()