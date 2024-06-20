import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_database_exists(conn):
    query = '''
    IF DB_ID('dms') IS NOT NULL
        --code mine :)
        print 'db exists'
    '''

    return pd.read_sql(query, con=conn)

