TASKS = 1
SEGMENTS_NO = 11
HOST='localhost'

PORT=5432
DBNAME='mn_dataset'
USER='my_user'
PASSWORD='password123'


# PORT=15433
# DBNAME='newron'
# USER='flyway'
# PASSWORD='8P87PE8HKuvjQaAP'


CONNECT_TIMEOUT=600

QUERY1 = '''
select 

    123456789 as item_id,
    'm$$$' as brand_name,
    true as is_exclusive,
    0.409 as units,
    0.567 as revenue,
    0.999 as abs_price,
    'aaaa' as segment,
    TRUE as matches_filter

from (select pg_sleep(5)) xxx
'''

QUERY3 = '''
 select * from t1 LIMIT 10000
'''

QUERY2 = '''
 select 

    123456789 as item_id,
    'm$$$' as brand_name,
    true as is_exclusive,
    0.409 as units,
    0.567 as revenue,
    0.999 as abs_price,
    'aaaa' as segment,
    TRUE as matches_filter

from generate_series(1, 10000)
'''


MYSQL_QUERY = '''
select 

    123456789 as item_id,
    'm$$$' as brand_name,
    true as is_exclusive,
    0.409 as units,
    0.567 as revenue,
    0.999 as abs_price,
    'aaaa' as segment,
    TRUE as matches_filter

from t1
limit 10000
'''