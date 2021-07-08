# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import time
from queue import Empty
from timeit import default_timer

import pandas as pd
from psycopg2.pool import ThreadedConnectionPool, SimpleConnectionPool

import config
import multiprocessing as mp


def pandas_worker(q_in : mp.Queue, q_out : mp.Queue):
    pid = mp.current_process()
    print(f"spawned {pid}")

    the_conn_pool = SimpleConnectionPool(
        minconn=1,
        maxconn=1,
        host=config.HOST,
        port=config.PORT,
        dbname=config.DBNAME,
        user=config.USER,
        password=config.PASSWORD,
        connect_timeout=config.CONNECT_TIMEOUT
    )

    while True:
        if q_in.empty():
            time.sleep(0.01)
        else:
            task = q_in.get()
            if task["segment"] == 'poison_pill':
                break
            with the_conn_pool.getconn() as conn:
                start = default_timer()
                data: pd.DataFrame = pd.read_sql_query(sql=task['sql'], con=conn)
                end = default_timer()
                print(f'pid {pid} processed {task["segment"]} took : {end - start:.5f}')
                q_out.put({'data': data, 'segment': task['segment']})
            the_conn_pool.putconn(conn)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    PROCESSES_SPAWNED = mp.cpu_count() - 1
    ctx = mp.get_context('spawn')
    q_in = ctx.Queue()
    q_out = ctx.Queue()
    processes = [ctx.Process(target=pandas_worker, args=(q_in, q_out)) for _ in range(0, PROCESSES_SPAWNED)]
    for p in processes:
        p.start()

    start = default_timer()
    for segment in range(0, config.SEGMENTS_NO):
        q_in.put({'sql': config.QUERY2, 'segment': str(segment)})
    end = default_timer()
    print(f'Added : {end - start:.5f}')

    start = default_timer()
    pandas_results =  [q_out.get() for _ in range(0, config.SEGMENTS_NO)]
    end = default_timer()
    print(f'Retrieved : {end - start:.5f}')

    cat_pandas = pd.concat([xxx['data'] for xxx in pandas_results])
    print(f"Processed {len(cat_pandas)}")

    for p in processes:
        q_in.put({'sql': None, 'segment': 'poison_pill'})

    for p in processes:
        p.join()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
