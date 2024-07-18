import click
from delta_maintenance.core.core import hello_world

@click.command()
@click.argument('arg1')
@click.argument('arg2')
def cli(arg1, arg2):
    """Simple program that processes ARG1 and ARG2."""
    result = hello_world(arg1)
    click.echo(result)

if __name__ == '__main__':
    cli()