import logging 
import click

from normalize_cli.core.logger import configure_logger
from normalize_cli.core.connection import SQLConnection
from normalize_cli.core.utils import check_database_exists
from normalize_cli.cli.outputs import init_cli

configure_logger()

logger = logging.getLogger(__name__)

@click.command()
@click.option('--server', help='A sacredheart domain server name or IP address where the SHUDW database resides')
@click.option('--db', help='Database being replicated.')
@click.option('--db_type', help='Database type. Options: [sqlserver, oracle, mysql, postgresql]')
def run(server, db, db_type):
    """
    Generates normalize dbt models.
    """
    init_cli()
    engine = SQLConnection(server, db, db_type)
    engine.construct_uri()
    engine.create_connection()
    engine.test_connection()


    



