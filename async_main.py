# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import asyncio
from timeit import default_timer
from typing import Dict

import pandas as pd

import config
import asyncpg

async def pool_creator():
    the_pool = await asyncpg.create_pool(
        min_size=config.TASKS,
        max_size=config.TASKS,
        host=config.HOST,
        port=config.PORT,
        database=config.DBNAME,
        user=config.USER,
        password=config.PASSWORD,
        timeout=config.CONNECT_TIMEOUT
    )

    return the_pool

async def pandas_worker(the_pool, sql_task: Dict, my_data):
    segment = sql_task['segment']
    print(f"Segment {segment}")
    async with the_pool.acquire() as connection:
        start = default_timer()
        stmt = await connection.prepare(sql_task['sql'])
        columns = [a.name for a in stmt.get_attributes()]
        data = await stmt.fetch()
        end = default_timer()
        print(f'Fetched {segment} : {end - start:.5f}')
        my_data[segment] = {'data': pd.DataFrame(data, columns=columns), 'segment': segment}

async def process_tasks(the_pool, my_data):
    pandas_results = []
    for segment in range(0, config.SEGMENTS_NO):
        await pandas_worker(the_pool, {'sql': config.QUERY2, 'segment': segment}, my_data)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    my_data = [None] * config.SEGMENTS_NO
    loop = asyncio.get_event_loop()
    the_pool = loop.run_until_complete(pool_creator())
    start = default_timer()
    loop.run_until_complete(process_tasks(the_pool, my_data))
    cat_pandas = pd.concat([xxx['data'] for xxx in my_data])
    print(f"Processed {len(cat_pandas)}")
    end = default_timer()
    print(f'Consumed : {end - start:.5f}')



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
