from asyncio.constants import ACCEPT_RETRY_DELAY
import logging
from sqlalchemy import create_engine

from normalize_cli.cli.outputs import report_invalid_db_type
from normalize_cli.core.utils import check_database_exists


logger = logging.getLogger(__name__)

VALID_DB_TYPES = ['sqlserver', 'oracle', 'mysql', 'postgresql']

class SQLConnection():
    def __init__(
            self,
            server: str,
            db: str,
            db_type: str

        ) -> None:
        self.server = server
        self.db = db
        self.db_type = db_type

    def construct_uri(self):
        if self.db_type not in VALID_DB_TYPES:
            raise SystemExit(report_invalid_db_type(self.db_type))
        else:
            if self.db_type == 'sqlserver':
                logger.info('Constructing uri.')
                logger.debug(f'db_type: {self.db_type}')
                self.uri = f'mssql+pyodbc://@{self.server}.sacredheart.edu/{self.db}?driver=ODBC+Driver+13+for+SQL+Server'
                logger.info('Successfully constructed uri.')
                logger.debug(f'constructed uri: {self.uri}')


    def create_connection(self): 
        logger.info('Creating connection')
        self.conn = create_engine(self.uri)
        logger.debug(f'engine: {self.conn}')
        logger.info('Successfully created connection')

        

