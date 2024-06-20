import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_database_exists(conn):
    query = '''
    IF DB_ID('dms') IS NOT NULL
        --code mine :)
        print 'db exists'
    '''

    logger.info('Running query.')
    result = pd.read_sql(query, con=conn)
    logger.info('Query completed successfully.')
    logger.debug(f'Query result: {result}')

    return result

