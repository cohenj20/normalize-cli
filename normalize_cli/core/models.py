from functools import cached_property
import logging
from sys import int_info
import pandas as pd
from typing import Dict

logger = logging.getLogger(__name__)

TOOLS_CONFIG = {
    'airbyte' : {
        'json_column' : '_airbyte_data',
    }
}

class Metadata():
    def __init__(
        self,
        conn,
        views,
        target_db,
        target_schema
    ) -> None:
        self.conn = conn
        self.views = views
        self.target_db = target_db
        self.target_schema = target_schema

    def query_metadata(self):
        if self.views == 'true':
            logger.info('Querying table and view metadata from source database.')
            query = '''
            SELECT 
                o.name AS objectname, 
                s.name AS objectschema,
                o.type_desc AS objecttype,
                c.name AS columnname, 
                UPPER(ty.name) AS type,
                ty.max_length AS maxlength,
                ty.precision AS precision,
                CASE WHEN ty.is_nullable = 0 THEN 'False' ELSE 'True' END AS nullable,
                CAST(sep.value AS VARCHAR) AS columndescription
            FROM sys.columns c WITH (NOLOCK) 
            JOIN sys.objects o WITH (NOLOCK) on o.object_id = c.object_id
            JOIN sys.types ty WITH (NOLOCK) on c.system_type_id = ty.system_type_id
            JOIN sys.schemas s WITH (NOLOCK) on s.schema_id = o.schema_id
            LEFT JOIN stage.dimSchemaScripts scripts WITH (NOLOCK) ON scripts.ObjectName = o.name AND scripts.SchemaName = s.name
            LEFT JOIN sys.extended_properties sep WITH (NOLOCK) on o.object_id = sep.major_id AND c.column_id = sep.minor_id AND sep.name = 'MS_Description'
            WHERE o.type_desc IN (
                'VIEW',
                'USER_TABLE'
            ) AND o.name NOT LIKE '%BrandonTest%'
            '''
        else:
            logger.info('Querying only table metadata from source database.')
            query = '''
            SELECT 
                o.name AS objectname, 
                s.name AS objectschema,
                o.type_desc AS objecttype,
                c.name AS columnname, 
                UPPER(ty.name) AS type,
                ty.max_length AS maxlength,
                ty.precision AS precision,
                CASE WHEN ty.is_nullable = 0 THEN 'False' ELSE 'True' END AS nullable,
                CAST(sep.value AS VARCHAR) AS columndescription
            FROM sys.columns c WITH (NOLOCK) 
            JOIN sys.objects o WITH (NOLOCK) on o.object_id = c.object_id
            JOIN sys.types ty WITH (NOLOCK) on c.system_type_id = ty.system_type_id
            JOIN sys.schemas s WITH (NOLOCK) on s.schema_id = o.schema_id
            LEFT JOIN stage.dimSchemaScripts scripts WITH (NOLOCK) ON scripts.ObjectName = o.name AND scripts.SchemaName = s.name
            LEFT JOIN sys.extended_properties sep WITH (NOLOCK) on o.object_id = sep.major_id AND c.column_id = sep.minor_id AND sep.name = 'MS_Description'
                AND o.type_desc IN (
                    'USER_TABLE'
                )
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
        logger.info(column)
        if i == 0:
            logger.debug(f'i = 1')
            line = f"    CAST(JSON_VALUE({self.target_schema}, '$.{column['columnname']}') AS {column['type']}) AS {column['columnname']}"
        elif i > 0:
            logger.debug(f'i = {i}')
            line = f"   ,CAST(JSON_VALUE({self.target_schema}, '$.{column['columnname']}') AS {column['type']}) AS {column['columnname']}"
        else:
            pass
        
        return line




        

    def generate_models(self):
        for object in self.unique_metadata_objects:
            columns = pd.DataFrame(self.full_metadata.loc[object]).to_dict(orient='records')
            model_lines = ['SELECT']
            for i, column in enumerate(columns):
                line = self.construct_normalization_lines(i,column)
                model_lines.append(line)

        return model_lines


            

        