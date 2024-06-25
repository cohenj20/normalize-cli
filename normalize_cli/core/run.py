import logging 
import click

from normalize_cli.core.logger import configure_logger
from normalize_cli.core.connection import SQLConnection
from normalize_cli.cli.outputs import init_cli
from normalize_cli.core.models import Metadata

configure_logger()

logger = logging.getLogger(__name__)

@click.command()
@click.option('--tool', help='Replication platofrm being used. Options: [airbyte]')
@click.option('--server', help='A sacredheart domain server name or IP address where the SHUDW database resides')
@click.option('--source_db', help='Source database being replicated.')
@click.option('--target_db', help='Target database to which data is being replicated.')
@click.option('--target_schema', help='Target database to which data is being replicated.')
@click.option('--db_type', help='Database type. Options: [sqlserver, oracle, mysql, postgresql]')
@click.option('--views', help='Boolean flag for inclusion of views in metadata retrieval. Options: [true, false]')
def run(tool, server, source_db, target_db, target_schema, db_type, views):
    """
    Generates normalize dbt models.
    """
    init_cli()
    engine = SQLConnection(server, source_db, db_type)
    engine.construct_uri()
    engine.create_connection()
    engine.test_connection()
    metadata = Metadata(engine.conn, views=views, target_db = target_db, target_schema=target_schema)
    metadata.generate_models()


    



