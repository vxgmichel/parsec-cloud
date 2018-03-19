import click

from parsec.core.cli import core_cmd
from parsec.backend.cli import backend_cmd
from parsec.ui import shell
try:
    from parsec.ui.fuse import cli as fuse_cli
except (ImportError, EnvironmentError) as exc:
    @click.command()
    def fuse_cli():
        raise RuntimeError('No available (%s), is fusepy&libfuse installed ?' % exc)


@click.group()
def cli():
    pass


cli.add_command(core_cmd, 'core')
cli.add_command(backend_cmd, 'backend')
cli.add_command(fuse_cli, 'fuse')
cli.add_command(shell.cli, 'shell')


if __name__ == '__main__':
    cli()
