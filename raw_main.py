# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import itertools

from psycopg2.pool import ThreadedConnectionPool

from concurrent import futures
from timeit import default_timer
from typing import Dict, List, Tuple
import config
import pandas as pd

def pandas_per_segment(the_conn_pool, segment)-> List[Tuple]:
    print(f"TASK is {segment}")
    sql_query = config.QUERY2
    with the_conn_pool.getconn() as conn:
        conn.set_session(readonly=True, autocommit=True)
        start = default_timer()
        with conn.cursor() as curs:
            curs.execute(sql_query)
            data = curs.fetchall()
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
    start = default_timer()
    master_list = [task.result() or task in tasks]
    result = pd.DataFrame(itertools.chain(*master_list), columns=['item_id', 'brand_name', 'is_exclusive', 'units', 'revenue', 'abs_price', 'segment', 'matches_filter'])
    end = default_timer()
    print(f'Chained : {end - start:.5f}')
    return result

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
