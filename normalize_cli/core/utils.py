import logging
import pandas as pd

logger = logging.getLogger(__name__)

def check_database_exists(conn, db):
    query = f'''
    SELECT name 
    from {db}.sys.tables
    '''

    logger.info('Checking if database exists.')
    result = pd.read_sql(query, conn)
    logger.info('Check ran successfully.')
    logger.debug(f'Query result:\n {result}')

    return result

def write_lines_to_file(lines, filename):
    with open(filename, 'w') as file:
        for line in lines:
            file.write(line + '\n')

