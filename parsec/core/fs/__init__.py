import inspect

from parsec.core.fs.beacon_monitor import BeaconMonitor
from parsec.core.fs.sync_monitor import SyncMonitor
from parsec.core.fs.local_folder_fs import (
    FSInvalidPath,  # noqa, republishing
    FSManifestLocalMiss,
    LocalFolderFS,
)
from parsec.core.fs.local_file_fs import LocalFileFS, FSBlocksLocalMiss
from parsec.core.fs.syncer import Syncer
from parsec.core.fs.remote_loader import RemoteLoader


class FS:
    def __init__(self, device, backend_conn, signal_ns):
        self._device = device

        self._local_file_fs = LocalFileFS(device, signal_ns)
        self._local_folder_fs = LocalFolderFS(device, signal_ns)
        self._remote_loader = RemoteLoader(backend_conn, device.local_db)
        self._syncer = Syncer(device, backend_conn, self._local_folder_fs, signal_ns)

        self._beacon_monitor = BeaconMonitor(device, device.local_db, signal_ns)
        self._sync_monitor = SyncMonitor(self._local_folder_fs, self._syncer, signal_ns)

    async def init(self, nursery):
        await self._beacon_monitor.init(nursery)
        await self._sync_monitor.init(nursery)

    async def teardown(self):
        await self._sync_monitor.teardown()
        await self._beacon_monitor.teardown()

    async def _load_and_retry(self, fn, *args, **kwargs):
        while True:
            try:
                if inspect.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)
                else:
                    return fn(*args, **kwargs)

            except FSManifestLocalMiss as exc:
                await self._remote_loader.load_manifest(exc.access)

            except FSBlocksLocalMiss as exc:
                for access in exc.accesses:
                    await self._remote_loader.load_block(access)

    async def stat(self, path):
        return await self._load_and_retry(self._local_folder_fs.stat, path)

    async def file_write(self, path, content, offset=0):
        fd = await self.file_fd_open(path)
        try:
            if offset:
                await self.file_fd_seek(fd, offset)
            await self.file_fd_write(fd, content)
        finally:
            await self.file_fd_close(fd)

    async def file_truncate(self, path, length):
        fd = await self.file_fd_open(path)
        try:
            await self.file_fd_truncate(fd, length)
        finally:
            await self.file_fd_close(fd)

    async def file_read(self, path, size=-1, offset=0):
        fd = await self.file_fd_open(path)
        try:
            if offset:
                await self.file_fd_seek(fd, offset)
            return await self.file_fd_read(fd, size)
        finally:
            await self.file_fd_close(fd)

    async def file_fd_open(self, path):
        access = await self._load_and_retry(self._local_folder_fs.get_access, path)
        return self._local_file_fs.open(access)

    async def file_fd_close(self, fd):
        self._local_file_fs.close(fd)

    async def file_fd_seek(self, fd, offset):
        self._local_file_fs.seek(fd, offset)

    async def file_fd_truncate(self, fd, length):
        self._local_file_fs.truncate(fd, length)

    async def file_fd_write(self, fd, content):
        self._local_file_fs.write(fd, content)

    async def file_fd_flush(self, fd):
        self._local_file_fs.flush(fd)

    async def file_fd_read(self, fd, size=-1):
        return await self._load_and_retry(self._local_file_fs.read, fd, size)

    async def file_create(self, path):
        await self._load_and_retry(self._local_folder_fs.touch, path)

    async def folder_create(self, path):
        await self._load_and_retry(self._local_folder_fs.mkdir, path)

    async def move(self, src, dst):
        await self._load_and_retry(self._local_folder_fs.move, src, dst)

    async def delete(self, path):
        await self._load_and_retry(self._local_folder_fs.delete, path)

    async def sync(self, path, recursive=True):
        print(id(self), "sync", path)
        sync_path, sync_recursive = self._local_folder_fs.get_sync_strategy(path, recursive)
        access = await self._load_and_retry(self._local_folder_fs.get_access, sync_path)
        notify = self._local_folder_fs.get_beacons(sync_path)
        await self._load_and_retry(
            self._syncer.sync, access, recursive=sync_recursive, notify=notify
        )
