from functools import cached_property
import logging
from select import select
from sys import int_info
import pandas as pd
from typing import Dict

from normalize_cli.core.utils import write_lines_to_file


logger = logging.getLogger(__name__)

TOOLS_CONFIG = {
    'airbyte' : {
        'json_column' : '_airbyte_data',
        'destination_table_prefix' : '_airbyte_raw_'
    }
}

class Metadata():
    def __init__(
        self,
        conn,
        views,
        target_db,
        target_schema,
        tool
    ) -> None:
        logger.info(f'Initializing Metadata instance...')
        self.conn = conn
        logger.debug(f'self.conn = {conn}')
        self.views = views
        logger.debug(f'self.views = {views}')
        self.target_db = target_db
        logger.debug(f'self.target_db = {target_db}')
        self.target_schema = target_schema
        logger.debug(f'self.target_schema = {target_schema}')
        self.tool = tool
        logger.debug(f'self.tool = {tool}')
    



    def query_metadata(self):
        if self.views == 'true':
            logger.info('Querying table and view metadata from source database.')
            query = '''
            SELECT 
                o.name AS objectname, 
                s.name AS objectschema,
                o.type_desc AS objecttype,
                c.name AS columnname, 
                CASE WHEN ty.name = 'datetime' THEN 'DATETIME2' ELSE UPPER(ty.name) END AS type,
                ty.max_length AS maxlength,
                ty.precision AS precision,
                CASE WHEN ty.is_nullable = 0 THEN 'False' ELSE 'True' END AS nullable
            FROM sys.columns c WITH (NOLOCK) 
            JOIN sys.objects o WITH (NOLOCK) on o.object_id = c.object_id
            JOIN sys.types ty WITH (NOLOCK) on c.system_type_id = ty.system_type_id
            JOIN sys.schemas s WITH (NOLOCK) on s.schema_id = o.schema_id
        
            WHERE o.type_desc IN (
                'VIEW',
                'USER_TABLE'
            ) 
                AND ty.is_user_defined = 0
                AND ty.user_type_id <> '256'
            '''
        else:
            logger.info('Querying only table metadata from source database.')
            query = '''
            SELECT 
                o.name AS objectname, 
                s.name AS objectschema,
                o.type_desc AS objecttype,
                c.name AS columnname, 
                CASE WHEN ty.name = 'datetime' THEN 'DATETIME2' ELSE UPPER(ty.name) END AS type,
                ty.max_length AS maxlength,
                ty.precision AS precision,
                CASE WHEN ty.is_nullable = 0 THEN 'False' ELSE 'True' END AS nullable
            FROM sys.columns c WITH (NOLOCK) 
            JOIN sys.objects o WITH (NOLOCK) on o.object_id = c.object_id
            JOIN sys.types ty WITH (NOLOCK) on c.system_type_id = ty.system_type_id
            JOIN sys.schemas s WITH (NOLOCK) on s.schema_id = o.schema_id
            WHERE o.type_desc = 'USER_TABLE'
                AND ty.is_user_defined = 0
                AND ty.user_type_id <> '256'
            '''

        metadata = pd.read_sql(query, self.conn)
        logger.info('Metadata query executed successfully')
        logger.debug(f'Sample of metadata results:\n {metadata.head(5)}')
        logger.debug(f'Metadata results size: {len(metadata)} rows')

        return metadata

    @cached_property
    def full_metadata(self):
        logger.info('Retrieving and caching full metadata from database.')
        full_metadata = self.query_metadata()
        full_metadata.index = full_metadata['objectname'].to_list()
        logger.info('Retrieved and cached full metadata.')

        return full_metadata

    @cached_property
    def unique_metadata_objects(self):
        logger.info('Caching unique metadata objects from database.')
        unique_metadata_objects = self.full_metadata['objectname'].unique()
        logger.info('Cached unique metadata objects from database.')

        return unique_metadata_objects

    def construct_normalization_lines(
        self,
        i: int,
        column: Dict[str, str]
        ):

        try:
            logger.info('Constructing column line for model file.')
            if i == 0:
                logger.debug(f'column index = 0')
                line = f"    CAST(JSON_VALUE({TOOLS_CONFIG[self.tool]['json_column']}, '$.{column['columnname']}') AS {column['type']}) AS {column['columnname']}"
            else:
                logger.debug(f'column index = {i}')
                line = f"   ,CAST(JSON_VALUE({TOOLS_CONFIG[self.tool]['json_column']}, '$.{column['columnname']}') AS {column['type']}) AS {column['columnname']}"
            
        except:
            logger.warn(f'{column} does not contain proper attributes')
            line = ''
        
        return line

    def construct_select_line(self):
        logger.info('Constructing SELECT line for model file.')
        select_line = 'SELECT'
        logger.info('Successfully constructed SELECT line for model file.')

        return select_line

    def construct_from_line(
        self,
        object: str
        ):
        try:
            logger.info('Constructing FROM line for model file.')
            from_line = f"FROM [{self.target_db}].[{self.target_schema}].[{TOOLS_CONFIG[self.tool]['destination_table_prefix'] + object}]"
            logger.info('Successfully constructed FROM line.')

        except Exception as E:
            logger.warn(f'Failed to construct FROM line: \n{E}')

        return from_line

    def generate_models(self):
        logger.info('Iterating through objects retrieved from source database.')
        for object in self.unique_metadata_objects:
            columns = pd.DataFrame(self.full_metadata.loc[object]).to_dict(orient='records')
            model_lines = []
            select_line = self.construct_select_line()
            model_lines.append(select_line)

            logger.info('Iterating through object columns.')
            for i, column in enumerate(columns):
                column_line = self.construct_normalization_lines(i,column)
                model_lines.append(column_line)
            from_line = self.construct_from_line(object=object)
            model_lines.append(from_line)

            write_lines_to_file(model_lines, filename=f'{self.tool}_normalized_{object}.sql')

        return logger.info(model_lines)


            

        