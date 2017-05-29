import os
import stat
import sys
import socket
import json
import click
import threading
from base64 import decodebytes, encodebytes
from errno import ENOENT, EBADFD
from stat import S_IRWXU, S_IRWXG, S_IRWXO, S_IFDIR, S_IFREG
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from parsec.tools import logger


DEFAULT_CORE_UNIX_SOCKET = '/tmp/parsec'


@click.command()
@click.argument('mountpoint', type=click.Path(exists=True, file_okay=False))
@click.option('--debug', '-d', is_flag=True, default=False)
@click.option('--nothreads', is_flag=True, default=False)
@click.option('--socket', '-s', default=DEFAULT_CORE_UNIX_SOCKET,
              help='Path to the UNIX socket (default: %s).' % DEFAULT_CORE_UNIX_SOCKET)
def cli(mountpoint, debug, nothreads, socket):
    # Do the import here in case fuse is not an available dependency
    start_fuse(socket, mountpoint, debug=debug, nothreads=nothreads)


class FuseOperations(LoggingMixIn, Operations):

    def __init__(self, socket_path):
        self._socket_path = socket_path
        self._socket_lock = threading.Lock()
        self._socket = None

    @property
    def sock(self):
        if not self._socket:
            self._init_socket()
        return self._socket

    def _init_socket(self):
        sock = socket.socket(socket.AF_UNIX, type=socket.SOCK_STREAM)
        if (not os.path.exists(self._socket_path) or
                not stat.S_ISSOCK(os.stat(self._socket_path).st_mode)):
            logger.error("File %s doesn't exist or isn't a socket. Is Parsec Core running?" %
                         self._socket_path)
            sys.exit(1)
        sock.connect(self._socket_path)
        logger.debug('Init socket')
        self._socket = sock

    def send_cmd(self, **msg):
        with self._socket_lock:
            req = json.dumps(msg).encode() + b'\n'
            logger.debug('Send: %r' % req)
            self.sock.send(req)
            raw_reps = self.sock.recv(4096)
            while raw_reps[-1] != ord(b'\n'):
                raw_reps += self.sock.recv(4096)
            logger.debug('Received: %r' % raw_reps)
            return json.loads(raw_reps[:-1].decode())

    def getattr(self, path, fh=None):
        response = self.send_cmd(cmd='path_info', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        if response['type'] == 'file':
            response = self.send_cmd(cmd='file_stat', id=response['id'])
            if response['status'] != 'ok':
                raise FuseOSError(ENOENT)
            stat = response
            stat['is_dir'] = False  # TODO remove this ?
        else:
            # TODO remove this?
            stat = {'is_dir': True, 'size': 0, 'ctime': 0, 'mtime': 0, 'atime': 0}
        fuse_stat = {
            'st_size': stat['size'],
            'st_ctime': stat['ctime'],  # TODO change to local timezone
            'st_mtime': stat['mtime'],
            'st_atime': stat['atime'],
        }
        # Set it to 777 access
        fuse_stat['st_mode'] = 0
        if stat['is_dir']:
            fuse_stat['st_mode'] |= S_IFDIR
        else:
            fuse_stat['st_mode'] |= S_IFREG
        fuse_stat['st_mode'] |= S_IRWXU | S_IRWXG | S_IRWXO
        fuse_stat['st_nlink'] = 1
        fuse_stat['st_uid'] = os.getuid()
        fuse_stat['st_gid'] = os.getgid()
        return fuse_stat

    def readdir(self, path, fh):
        response = self.send_cmd(cmd='path_info', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return ['.', '..'] + response['items']

    def create(self, path, mode):
        response = self.send_cmd(cmd='file_create', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return self.open(path)

    def open(self, path, flags=0):
        response = self.send_cmd(cmd='file_open', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return response['fd']

    def release(self, path, fh):
        response = self.send_cmd(cmd='file_close', fd=fh)
        if response['status'] != 'ok':
            raise FuseOSError(EBADFD)

    def read(self, path, size, offset, fh):
        response = self.send_cmd(cmd='file_read', fd=fh, size=size, offset=offset)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return decodebytes(response['content'].encode())

    def write(self, path, data, offset, fh):
        length = len(data)
        data = encodebytes(data).decode()
        response = self.send_cmd(cmd='file_write', fd=fh, data=data, offset=offset)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return length

    def truncate(self, path, length, fh=None):
        release_fh = False
        if not fh:
            fh = self.open(path, flags=0)
            release_fh = True
        try:
            response = self.send_cmd(cmd='file_truncate', fd=fh, length=length)
            if response['status'] != 'ok':
                raise FuseOSError(ENOENT)
        finally:
            if release_fh:
                self.release(path, fh)

    def unlink(self, path):
        response = self.send_cmd(cmd='file_delete', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)

    def mkdir(self, path, mode):
        response = self.send_cmd(cmd='folder_create', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return 0

    def rmdir(self, path):
        response = self.send_cmd(cmd='folder_delete', path=path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return 0

    def rename(self, old_path, new_path):
        response = self.send_cmd(cmd='file_rename',
                                 old_path=old_path,
                                 new_path=new_path)
        if response['status'] != 'ok':
            raise FuseOSError(ENOENT)
        return 0

    def flush(self, path, fh):
        # TODO flush file in core
        return 0

    def fsync(self, path, datasync, fh):
        return 0  # TODO

    def fsyncdir(self, path, datasync, fh):
        return 0  # TODO


def start_fuse(socket_path: str, mountpoint: str, debug: bool=False, nothreads: bool=False):
    operations = FuseOperations(socket_path)
    FUSE(operations, mountpoint, foreground=True, nothreads=nothreads, debug=debug)
