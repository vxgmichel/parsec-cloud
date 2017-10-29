from os import environ
import sys
from importlib import import_module
import click
from logbook import WARNING

from parsec.backend import run_backend


# TODO: remove me once RSA key loading and backend handling are easier
JOHN_DOE_IDENTITY = 'John_Doe'
JOHN_DOE_PRIVATE_KEY = b"""
"""
JOHN_DOE_PUBLIC_KEY = b"""
"""

DEFAULT_CORE_UNIX_SOCKET = '/tmp/parsec'


@click.group()
def cli():
    pass


@click.command()
@click.argument('id')
@click.argument('args', nargs=-1)
@click.option('socket_path', '--socket', '-s', default=DEFAULT_CORE_UNIX_SOCKET,
              help='Path to the UNIX socket (default: %s).' % DEFAULT_CORE_UNIX_SOCKET)
def cmd(id, args, socket_path, per_cmd_connection):
    pass


@click.command()
@click.option('--socket', '-s', default=DEFAULT_CORE_UNIX_SOCKET,
              help='Path to the UNIX socket (default: %s).' % DEFAULT_CORE_UNIX_SOCKET)
def shell(socket):
    pass


@click.command()
@click.option('--socket', '-s', default=DEFAULT_CORE_UNIX_SOCKET,
              help='Path to the UNIX socket exposing the core API (default: %s).' %
              DEFAULT_CORE_UNIX_SOCKET)
@click.option('--backend-host', '-H', default='tcp://127.0.0.1:6777')
@click.option('--backend-watchdog', '-W', type=click.INT, default=None)
@click.option('--debug', '-d', is_flag=True)
@click.option('--identity', '-i', default=None)
@click.option('--identity-key', '-I', type=click.File('rb'), default=None)
@click.option('--I-am-John', is_flag=True, help='Log as dummy John Doe user')
@click.option('--cache-size', help='Max number of elements in cache', default=1000)
def core(**kwargs):
    pass


@click.command()
@click.option('--pubkeys', default=None)
@click.option('--host', '-H', default=None, help='Host to listen on (default: 127.0.0.1)')
@click.option('--port', '-P', default=None, type=int, help=('Port to listen on (default: 6777)'))
@click.option('--no-client-auth', is_flag=True,
              help='Disable authentication handshake on client connection (default: false)')
@click.option('--store', '-s', default=None, help="Store configuration (default: in memory)")
@click.option('--block-store', '-b', default=None,
    help="URL of the block store the clients should write into (default: "
    "backend creates it own in-memory block store).")
@click.option('--debug', '-d', is_flag=True)
def backend(host, port, pubkeys, no_client_auth, store, block_store, debug):
    host = host or environ.get('HOST', '127.0.0.1')
    port = port or int(environ.get('PORT', 6777))
    print('Starting parsec backend on %s:%s with store %s' % (host, port, 'foo'))
    try:
        run_backend(host, port, block_store)
    except KeyError:
        print('Bye ;-)')


@click.command()
@click.option('--socket', '-s', default=DEFAULT_CORE_UNIX_SOCKET,
              help='Path to the UNIX socket (default: %s).' % DEFAULT_CORE_UNIX_SOCKET)
@click.option('--identity', '-i', required=True)
@click.option('--key-size', '-S', type=int, default=2048)
def signup(socket, identity, key_size):
    pass


cli.add_command(cmd)
cli.add_command(shell)
cli.add_command(core)
cli.add_command(backend)
cli.add_command(signup)


def _add_command_if_can_import(path, name=None):
    module_path, field = path.rsplit('.', 1)
    try:
        module = import_module(module_path)
        cli.add_command(getattr(module, field), name=name)
    except (ImportError, AttributeError, EnvironmentError):
        pass


_add_command_if_can_import('parsec.backend.postgresql.cli', 'postgresql')
_add_command_if_can_import('parsec.ui.fuse.cli', 'fuse')


if __name__ == '__main__':
    cli()
