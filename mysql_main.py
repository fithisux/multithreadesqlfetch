# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import threading
from concurrent.futures import thread
from queue import Queue

import mysql.connector as mysql
from psycopg2.pool import ThreadedConnectionPool

import config
from concurrent import futures
from timeit import default_timer
from typing import Dict

import config
import pandas as pd

def pandas_per_segment(the_conn_pool, segment, results)-> pd.DataFrame:
    print(f"TASK is {segment}")
    sql_query = config.MYSQL_QUERY
    conn = the_conn_pool.pop()
    start = default_timer()
    data : pd.DataFrame = pd.read_sql_query(sql=sql_query, con=conn)
    end = default_timer()
    print(f'DB to retrieve {segment} took : {end - start:.5f}')
    the_conn_pool.append(conn)
    results[segment] = data

def get_sales(the_conn_pool) -> pd.DataFrame:
    start = default_timer()
    thread_list = []
    results = [None]*(config.SEGMENTS_NO)
    for segment in range(0, config.SEGMENTS_NO):
        x = threading.Thread(target=pandas_per_segment, args=(the_conn_pool, segment, results))
        x.start()
        thread_list.append(x)

    for t in thread_list:
        t.join()

    end = default_timer()
    print(f'Consumed : {end-start:.5f}')
    return pd.concat(results)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    stack = []
    for i in range(0, config.SEGMENTS_NO):
        conn = db = mysql.connect(
            host=config.HOST,
            database=config.DBNAME,
            user=config.USER,
            passwd=config.PASSWORD,
            connect_timeout=config.CONNECT_TIMEOUT,
            port=3306
        )
        stack.append(conn)

    cat_pandas = get_sales(stack)
    print(f"Processed {len(cat_pandas)}")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
