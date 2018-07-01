import attr
import pendulum
from collections import defaultdict
from typing import NewType
from math import inf

from parsec.signals import get_signal
from parsec.core.local_db import LocalDBMissingEntry
from parsec.core.fs.data import (
    is_file_manifest,
    new_access,
)
from parsec.core.fs.buffer_ordering import (
    quick_filter_block_accesses,
    Buffer,
    BaseOrderedSpace,
    UncontiguousSpace,
    ContiguousSpace,
    InBufferSpace,
    merge_buffers,
    merge_buffers_with_limits,
    merge_buffers_with_limits_and_alignment,
)
from parsec.core.fs.local_folder_fs import mark_manifest_modified


FileDescriptor = NewType('FileDescriptor', int)


def _shorten_data_repr(data):
    if len(data) > 100:
        return data[:40] + b"..." + data[-40:]
    else:
        return data


@attr.s(slots=True, repr=False)
class RamBuffer(Buffer):
    def __repr__(self):
        return "%s(start=%r, end=%r, data=%r)" % (
            type(self).__name__,
            self.start,
            self.end,
            _shorten_data_repr(self.data),
        )


@attr.s(slots=True)
class DirtyBlockBuffer(Buffer):
    access = attr.ib()
    data = attr.ib(default=None)


@attr.s(slots=True)
class BlockBuffer(Buffer):
    access = attr.ib()
    data = attr.ib(default=None)


@attr.s(slots=True)
class NullFillerBuffer(BaseOrderedSpace):
    @property
    def data(self):
        return bytearray(self.size)
    

class FSBlocksLocalMiss(Exception):
    def __init__(self, accesses):
        super().__init__(accesses)
        self.accesses = accesses


@attr.s(slots=True)
class FileCursor:
    access = attr.ib()
    offset = attr.ib(default=0)


@attr.s(slots=True)
class HotFile:
    size = attr.ib()
    pending_writes = attr.ib(factory=list)


class FSInvalidFileDescriptor(Exception):
    pass


class LocalFileFS:
    def __init__(self, device):
        self._local_db = device.local_db
        self._opened_cursors = {}
        self._hot_files = defaultdict(list)
        self._next_fd = 1

    def _get_cursor_from_fd(self, fd):
        try:
            return self._opened_cursors[fd]
        except KeyError:
            raise FSInvalidFileDescriptor(fd)

    def _get_quickly_filtered_blocks(self, manifest, start, end):
        dirty_blocks = [
            DirtyBlockBuffer(*x)
            for x in quick_filter_block_accesses(manifest["dirty_blocks"], start, end)
        ]
        blocks = [
            BlockBuffer(*x)
            for x in quick_filter_block_accesses(manifest["blocks"], start, end)
        ]

        return blocks + dirty_blocks

    def open(self, access) -> FileDescriptor:
        # Sanity check
        manifest = self._local_db.get(access)
        assert is_file_manifest(manifest)

        cursor = FileCursor(access)
        fd = self._next_fd
        self._opened_cursors[fd] = cursor
        self._next_fd += 1
        return fd

    def close(self, fd: FileDescriptor):
        self.flush(fd)
        del self._opened_cursors[fd]

    def seek(self, fd: FileDescriptor, offset: int):
        cursor = self._get_cursor_from_fd(fd)
        cursor.offset = offset

    def write(self, fd: FileDescriptor, content: bytes):
        cursor = self._get_cursor_from_fd(fd)

        if not content:
            return

        pending_writes = self._hot_files[cursor.access["id"]]
        start = cursor.offset
        end = start + len(content)
        pending_writes.append(RamBuffer(start, end, content))

        cursor.offset += len(content)

    def truncate(self, fd: FileDescriptor, length: int):
        cursor = self._get_cursor_from_fd(fd)

        manifest = self._local_db.get(cursor.access)
        assert is_file_manifest(manifest)

        start = 0
        end = length

        pending_writes = self._hot_files[cursor.access["id"]]

        blocks = self._get_quickly_filtered_blocks(manifest, start, end)
        blocks += pending_writes

        merged = merge_buffers_with_limits(blocks, start, end)
        assert merged.size <= length
        assert merged.start == start

        # Fill the gaps with buffers full of 0x00
        prev_cs = None
        cs_buffers = []
        for cs in merged.spaces:
            if prev_cs:
                cs_buffers.append(
                    InBufferSpace(prev_cs.end, cs.start, NullFillerBuffer(prev_cs.end, cs.start))
                )
            elif cs.start != 0:
                cs_buffers.append(
                    InBufferSpace(0, cs.start, NullFillerBuffer(0, cs.start))
                )
            prev_cs = cs
            cs_buffers += cs.buffers

        if end != inf:
            if cs_buffers and cs_buffers[-1].end != end:
                cs_buffers.append(
                    InBufferSpace(cs_buffers[-1].end, end, NullFillerBuffer(cs_buffers[-1].end, end))
                )

            if not merged.spaces:
                cs_buffers.append(
                    InBufferSpace(merged.start, merged.end, NullFillerBuffer(merged.start, merged.end))
                )

        blocks = []
        dirty_blocks = []
        for bs in cs_buffers:
            if isinstance(bs.buffer, DirtyBlockBuffer):
                dirty_blocks.append({
                    'id': bs.buffer.access['id'],
                    'key': bs.buffer.access['key'],
                    'offset': bs.start,
                    'size': bs.size,
                })

            elif isinstance(bs.buffer, BlockBuffer):
                blocks.append({
                    'id': bs.buffer.access['id'],
                    'key': bs.buffer.access['key'],
                    'offset': bs.start,
                    'size': bs.size,
                })

            else:
                block_access = new_access()
                self._local_db.set(block_access, bs.get_data())
                dirty_blocks.append(
                    {
                        "id": block_access["id"],
                        "key": block_access["key"],
                        "offset": bs.start,
                        "size": bs.size,
                    }
                )

        manifest['blocks'] = blocks
        manifest['dirty_blocks'] = dirty_blocks
        manifest['size'] = merged.size
        mark_manifest_modified(manifest)

        self._local_db.set(cursor.access, manifest)

        del self._hot_files[cursor.access["id"]]

        get_signal("fs.entry.modified").send("local", id=cursor.access["id"])

    def read(self, fd: FileDescriptor, size: int = inf):
        cursor = self._get_cursor_from_fd(fd)

        manifest = self._local_db.get(cursor.access)
        assert is_file_manifest(manifest)

        start = cursor.offset
        end = cursor.offset + size

        pending_writes = self._hot_files[cursor.access["id"]]

        blocks = self._get_quickly_filtered_blocks(manifest, start, end)
        blocks += pending_writes

        merged = merge_buffers_with_limits(blocks, start, end)
        assert merged.size <= size
        assert merged.start == start

        # Fill the gaps with buffers full of 0x00
        prev_cs = None
        cs_buffers = []
        for cs in merged.spaces:
            if prev_cs:
                cs_buffers.append(
                    InBufferSpace(prev_cs.end, cs.start, NullFillerBuffer(prev_cs.end, cs.start))
                )
            elif cs.start != start:
                cs_buffers.append(
                    InBufferSpace(start, cs.start, NullFillerBuffer(start, cs.start))
                )
            prev_cs = cs
            cs_buffers += cs.buffers

        if cs_buffers and cs_buffers[-1].end != end:
            for pw in pending_writes:
                if pw.start >= end:
                    # We are reading inside a hole
                    cs_buffers.append(
                        InBufferSpace(cs_buffers[-1].end, end, NullFillerBuffer(cs_buffers[-1].end, end))
                    )

        if not cs_buffers:
            for pw in pending_writes:
                if pw.start >= cursor.offset:
                    # We are reading inside a hole
                    cursor.offset += size
                    return bytearray(size)
            else:
                return b''

        end = cs_buffers[-1].end
        missing = []
        data = bytearray(end - start)
        for bs in cs_buffers:
            if isinstance(bs.buffer, DirtyBlockBuffer):
                access = bs.buffer.access
                try:
                    bs.buffer.data = self._local_db.get(access)[:bs.buffer.size]
                except LocalDBMissingEntry as exc:
                    raise RuntimeError(f"Unknown local block `{access['id']}`") from exc

            elif isinstance(bs.buffer, BlockBuffer):
                access = bs.buffer.access
                try:
                    bs.buffer.data = self._local_db.get(access)[:bs.buffer.size]
                except LocalDBMissingEntry:
                    missing.append(access)

            data[bs.start - start : bs.end - start] = bs.get_data()

        if missing:
            raise FSBlocksLocalMiss(missing)

        cursor.offset += len(data)
        return data

    def need_flush(self, fd):
        cursor = self._get_cursor_from_fd(fd)
        return bool(self._hot_files.get(cursor.access["id"], False))

    def flush(self, fd):
        if not self.need_flush(fd):
            return
        # Weird implementation...
        self.truncate(fd, inf)
