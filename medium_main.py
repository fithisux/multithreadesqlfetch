# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import tempfile

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

import config
from concurrent import futures
from timeit import default_timer
from typing import Dict

import config
import pandas as pd

def pandas_per_segment(the_conn_pool, segment)-> pd.DataFrame:
    print(f"TASK is {segment}")
    sql_query = config.QUERY2
    with the_conn_pool.getconn() as conn:
        conn.set_session(readonly=True, autocommit=True)
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=sql_query, head="HEADER"
        )
        start = default_timer()
        with tempfile.TemporaryFile() as tmpfile:
            with conn.cursor() as curs:
                curs.copy_expert(copy_sql, tmpfile, size=8192)
            tmpfile.seek(0)
            data = pd.read_csv(tmpfile)
        end = default_timer()
        print(f'DB to retrieve {segment} took : {end - start:.5f}')
    the_conn_pool.putconn(conn)
    return data


def get_sales(the_conn_pool) -> pd.DataFrame:
    tasks : Dict = {}
    start = default_timer()
    with futures.ThreadPoolExecutor(max_workers=config.TASKS) as executor:
        for segment in range(0, config.SEGMENTS_NO):
            task = executor.submit(pandas_per_segment,
                            the_conn_pool = the_conn_pool,
                            segment=segment)
            tasks[task] = segment

    end = default_timer()
    print(f'Consumed : {end-start:.5f}')
    return pd.concat([task.result() for task in tasks])

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connection_pool = ThreadedConnectionPool(
        minconn=config.TASKS,
        maxconn=config.TASKS,
        host=config.HOST,
        port=config.PORT,
        dbname=config.DBNAME,
        user=config.USER,
        password=config.PASSWORD,
        connect_timeout=config.CONNECT_TIMEOUT
    )
    get_sales(connection_pool)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
