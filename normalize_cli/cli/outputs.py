import click
import colorama

def init_cli():
    click.echo(f'======================================================================================================================')
    click.echo(f'                                                   Normalize                                                          ')
    click.echo(f'======================================================================================================================')
    click.echo(f'                                                                                                                      ')

def report_invalid_db_type(db_type):
    click.echo(f"The DB type you have entered ({db_type}) is invalid. Run 'normalize run -h' to see the list of valid db types")

