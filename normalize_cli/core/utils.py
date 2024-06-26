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
    with open(filename, 'w', encoding="utf-8") as file:
        logger.info('Iterating through lines.')
        for i, line in enumerate(lines):
            try:
                logger.info(f'Writing line {i+1} to {filename}')
                logger.debug(f'Line contents: {line}')
                file.write(line + '\n')
                logger.info(f'Successfully wrote line {i+1} to {filename}.')
            except Exception as E:
                logger.warn(f'Error writing line {i+1} to {filename}.')
