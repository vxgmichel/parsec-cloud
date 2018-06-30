import attr
import pendulum
from collections import defaultdict

from parsec.signals import get_signal
from parsec.core.local_db import LocalDBMissingEntry
from parsec.core.fs.data import (
    is_file_manifest,
    is_folder_manifest,
    new_access,
    new_local_folder_manifest,
    new_local_file_manifest,
)


def mark_manifest_modified(manifest):
    manifest["updated"] = pendulum.now()
    manifest["need_sync"] = True


def normalize_path(path):
    normalized = "/" + "/".join([x for x in path.split("/") if x])
    return normalized


class FSInvalidPath(Exception):
    pass


class LocalManifestFS:
    def __init__(self, device):
        self.local_author = device.user_id
        self.root_access = device.user_manifest_access
        self._local_db = device.local_db

        # Sanity check
        try:
            self._local_db.get(self.root_access)
        except LocalDBMissingEntry as exc:
            raise RuntimeError("Device %s is not initialized" % device) from exc

    def set_manifest(self, access, manifest):
        self._local_db.set(access, manifest)

    def get_manifest(self, access):
        return self._local_db.get(access)

    def mark_outdated_manifest(self, access):
        self._local_db.clear(access)

    def get_beacons(self, path):
        path = normalize_path(path)
        beacons = []

        def _beacons_collector(access, manifest):
            beacon_id = manifest.get("beacon_id")
            if beacon_id:
                beacons.append((beacon_id, access["key"]))

        self._retrieve_entry(path, collector=_beacons_collector)
        return beacons

    def get_entry_path(self, entry_id):
        if entry_id == self.root_access["id"]:
            return "/", self.root_access, self._local_db.get(self.root_access)

        # Brute force style
        def _recursive_search(access, path):
            manifest = self._local_db.get(access)
            if access["id"] == entry_id:
                return path, access, manifest

            if is_folder_manifest(manifest):
                for child_name, child_access in manifest["children"].items():
                    found = _recursive_search(child_access, "%s/%s" % (path, child_name))
                    if found:
                        return found

        found = _recursive_search(self.root_access, "")
        if not found:
            raise LocalDBMissingEntry(entry_id)
        return found

    def _retrieve_entry(self, path, collector=None):
        assert "//" not in path
        assert path.startswith("/")

        def _retrieve_entry_recursive(curr_access, hops):
            try:
                curr_manifest = self._local_db.get(curr_access)
            except KeyError:
                raise LocalDBMissingEntry(curr_access)

            if not hops:
                if collector:
                    collector(curr_access, curr_manifest)
                return curr_access, curr_manifest

            if not is_folder_manifest(curr_manifest):
                raise FSInvalidPath(path)

            if collector:
                collector(curr_access, curr_manifest)

            hop, *hops = hops
            try:
                return _retrieve_entry_recursive(curr_manifest["children"][hop], hops)

            except KeyError:
                raise FSInvalidPath(path)

        hops = [hop for hop in path.split("/") if hop]
        return _retrieve_entry_recursive(self.root_access, hops)

    def get_sync_strategy(self, path, recursive):
        path = normalize_path(path)
        # Consider root is never a placeholder
        curr_path = "/"
        curr_parent_path = ""
        for hop in path.split("/")[1:]:
            curr_parent_path = curr_path
            curr_path += hop if curr_path.endswith("/") else ("/" + hop)
            _, curr_manifest = self._retrieve_entry(curr_path)
            if curr_manifest["is_placeholder"]:
                sync_path = curr_parent_path
                break

        else:
            return path, recursive

        sync_recursive = {}
        curr_sync_recursive = sync_recursive
        for hop in path[len(curr_parent_path) :].split("/")[1:]:
            curr_sync_recursive[hop] = {}
            curr_sync_recursive = curr_sync_recursive[hop]
        return sync_path, sync_recursive

    def get_access(self, path):
        path = normalize_path(path)
        access, _ = self._retrieve_entry(path)
        return access

    def stat(self, path):
        path = normalize_path(path)
        access, manifest = self._retrieve_entry(path)
        if is_file_manifest(manifest):
            return {
                "type": "file",
                "created": manifest["created"],
                "updated": manifest["updated"],
                "base_version": manifest["base_version"],
                "is_placeholder": manifest["is_placeholder"],
                "need_sync": manifest["need_sync"],
                "size": manifest["size"],
            }

        else:
            return {
                "type": "folder",
                "created": manifest["created"],
                "updated": manifest["updated"],
                "base_version": manifest["base_version"],
                "is_placeholder": manifest["is_placeholder"],
                "need_sync": manifest["need_sync"],
                "children": list(sorted(manifest["children"].keys())),
            }

    def file_create(self, path):
        path = normalize_path(path)
        parent_path, child_name = path.rsplit("/", 1)
        access, manifest = self._retrieve_entry(parent_path or '/')
        if not is_folder_manifest(manifest):
            raise FSInvalidPath(path)

        child_access = new_access()
        child_manifest = new_local_file_manifest(self.local_author)
        manifest["children"][child_name] = child_access
        mark_manifest_modified(manifest)
        self._local_db.set(access, manifest)
        self._local_db.set(child_access, child_manifest)
        get_signal("fs.entry.modified").send("local", id=access["id"])
        get_signal("fs.entry.created").send("local", id=child_access["id"])

    def folder_create(self, path):
        path = normalize_path(path)
        parent_path, child_name = path.rsplit("/", 1)
        access, manifest = self._retrieve_entry(parent_path or '/')
        if not is_folder_manifest(manifest):
            raise FSInvalidPath(path)

        child_access = new_access()
        child_manifest = new_local_folder_manifest(self.local_author)
        manifest["children"][child_name] = child_access
        mark_manifest_modified(manifest)
        self._local_db.set(access, manifest)
        self._local_db.set(child_access, child_manifest)
        get_signal("fs.entry.modified").send("local", id=access["id"])
        get_signal("fs.entry.created").send("local", id=child_access["id"])

    def move(self, src, dst):
        src = normalize_path(src)
        dst = normalize_path(dst)
        parent_src, child_src = src.rsplit("/", 1)
        parent_dst, child_dst = dst.rsplit("/", 1)
        parent_src = parent_src or '/'
        parent_dst = parent_dst or '/'

        if parent_src == parent_dst:
            parent_access, parent_manifest = self._retrieve_entry(parent_src)
            if not is_folder_manifest(parent_manifest):
                raise FSInvalidPath(src)

            try:
                entry = parent_manifest["children"].pop(child_src)
            except KeyError:
                raise FSInvalidPath(src)
            parent_manifest["children"][child_dst] = entry
            mark_manifest_modified(parent_manifest)

            self._local_db.set(parent_access, parent_manifest)
            get_signal("fs.entry.modified").send("local", id=parent_access["id"])

        else:
            parent_src_access, parent_src_manifest = self._retrieve_entry(parent_src)
            if not is_folder_manifest(parent_src_manifest):
                raise FSInvalidPath(parent_src)
            parent_dst_access, parent_dst_manifest = self._retrieve_entry(parent_dst)
            if not is_folder_manifest(parent_src_manifest):
                raise FSInvalidPath(parent_dst)

            try:
                entry = parent_src_manifest["children"].pop(child_src)
            except KeyError:
                raise FSInvalidPath(src)
            parent_dst_manifest["children"][child_dst] = entry

            mark_manifest_modified(parent_src_manifest)
            mark_manifest_modified(parent_dst_manifest)

            self._local_db.set(parent_src_access, parent_src_manifest)
            self._local_db.set(parent_dst_access, parent_dst_manifest)

            get_signal("fs.entry.modified").send("local", id=parent_src_access["id"])
            get_signal("fs.entry.modified").send("local", id=parent_dst_access["id"])


class LocalFileFSMissingBlockEntries(Exception):
    def __init__(self, accesses):
        super().__init__(accesses)
        self.accesses = accesses


@attr.s(slots=True)
class FileCursor:
    access = attr.ib()
    offset = attr.ib(default=0)


class LocalFileFS:
    def __init__(self, local_db):
        self._local_db = local_db
        self._opened_cursors = {}
        self._hot_files = defaultdict(list)
        self._next_fd = 1

    def open(self, access):
        cursor = FileCursor(access)
        fd = self._next_fd
        self._opened_cursors[fd] = cursor
        self._next_fd += 1
        return fd

    def close(self, fd):
        self.flush(fd)
        del self._opened_cursors[fd]

    def seek(self, fd, offset):
        cursor = self._opened_cursors[fd]
        cursor.offset = offset

    def write(self, fd, content, offset=0):
        cursor = self._opened_cursors[fd]
        self._hot_files[cursor.access["id"]].append((content, offset, pendulum.now()))
        cursor.offset += len(content)

    def read(self, fd, size=-1, offset=0):
        cursor = self._opened_cursors[fd]

        manifest = self._local_db.get(cursor.access)
        assert is_file_manifest(manifest)
        data = bytearray()
        missing = []
        for block_access in manifest["blocks"]:
            try:
                block_content = self._local_db.get(block_access)
            except LocalDBMissingEntry:
                missing.append(block_access)
                continue
            data[
                block_access["offset"] : block_access["offset"] + block_access["size"]
            ] = block_content
        if missing:
            raise LocalFileFSMissingBlockEntries(missing)

        for block_access in manifest["dirty_blocks"]:
            try:
                block_content = self._local_db.get(block_access)
            except LocalDBMissingEntry as exc:
                raise RuntimeError() from exc
            data[
                block_access["offset"] : block_access["offset"] + block_access["size"]
            ] = block_content

        pending_writes = self._hot_files[cursor.access["id"]]
        for write_content, write_offset, _ in pending_writes:
            data[write_offset : write_offset + len(write_content)] = write_content

        if size < 0:
            data = data[offset:]
        else:
            data = data[offset : offset + size]

        cursor.offset += len(data)
        return data

    def need_flush(self, fd):
        cursor = self._opened_cursors[fd]
        return bool(self._hot_files[cursor.access["id"]])

    def flush(self, fd):
        cursor = self._opened_cursors[fd]
        access = cursor.access

        pending_writes = self._hot_files[access["id"]]
        if not pending_writes:
            return

        new_dirty_blocks = []
        block_max_end = 0
        for content, offset, _ in pending_writes:
            block_access = new_access()
            self._local_db.set(block_access, content)
            new_dirty_blocks.append(
                {
                    "id": block_access["id"],
                    "key": block_access["key"],
                    "offset": offset,
                    "size": len(content),
                }
            )
            block_max_end = max(block_max_end, offset + len(content))

        manifest = self._local_db.get(access)
        assert is_file_manifest(manifest)
        _, _, last_updated = pending_writes[-1]
        if last_updated > manifest["updated"]:
            manifest["updated"] = last_updated
        manifest["dirty_blocks"] += new_dirty_blocks
        manifest["size"] = max(block_max_end, manifest["size"])
        mark_manifest_modified(manifest)
        self._local_db.set(access, manifest)

        del self._hot_files[access["id"]]

        get_signal("fs.entry.modified").send("local", id=access["id"])
