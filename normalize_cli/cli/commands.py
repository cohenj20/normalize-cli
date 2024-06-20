import click
import colorama

from normalize_cli.core.run import run

colorama.init()

@click.group
def my_commands():
    """
    An internal tool for visualizing SHU data warehouse dependencies.
    """
    pass

my_commands.add_command(run)

            
if __name__ == "__main__":
    my_commands()