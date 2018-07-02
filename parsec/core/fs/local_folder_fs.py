import pendulum

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


class FSManifestLocalMiss(Exception):
    pass


class LocalFolderFS:
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
            raise FSManifestLocalMiss(entry_id)
        return found

    def _retrieve_entry(self, path, collector=None):
        assert "//" not in path
        assert path.startswith("/")

        def _retrieve_entry_recursive(curr_access, curr_path, hops):
            try:
                curr_manifest = self._local_db.get(curr_access)
            except LocalDBMissingEntry as exc:
                raise FSManifestLocalMiss(curr_access) from exc

            if not hops:
                if collector:
                    collector(curr_access, curr_manifest)
                return curr_access, curr_manifest

            if not is_folder_manifest(curr_manifest):
                raise NotADirectoryError(20, "Not a directory", curr_path)

            if collector:
                collector(curr_access, curr_manifest)

            hop, *hops = hops
            try:
                return _retrieve_entry_recursive(
                    curr_manifest["children"][hop], f"{curr_path}/{hop}", hops
                )

            except KeyError:
                raise FileNotFoundError(2, "No such file or directory", curr_path)

        hops = [hop for hop in path.split("/") if hop]
        return _retrieve_entry_recursive(self.root_access, "", hops)

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

    def touch(self, path):
        path = normalize_path(path)
        parent_path, child_name = path.rsplit("/", 1)
        access, manifest = self._retrieve_entry(parent_path or "/")
        if not is_folder_manifest(manifest):
            raise NotADirectoryError(20, "Not a directory", parent_path)
        if child_name in manifest["children"]:
            raise FileExistsError(17, "File exists", path)

        child_access = new_access()
        child_manifest = new_local_file_manifest(self.local_author)
        manifest["children"][child_name] = child_access
        mark_manifest_modified(manifest)
        self._local_db.set(access, manifest)
        self._local_db.set(child_access, child_manifest)
        get_signal("fs.entry.modified").send("local", id=access["id"])
        get_signal("fs.entry.created").send("local", id=child_access["id"])

    def mkdir(self, path):
        path = normalize_path(path)
        parent_path, child_name = path.rsplit("/", 1)
        access, manifest = self._retrieve_entry(parent_path or "/")
        if not is_folder_manifest(manifest):
            raise NotADirectoryError(20, "Not a directory", parent_path)
        if child_name in manifest["children"]:
            raise FileExistsError(17, "File exists", path)

        child_access = new_access()
        child_manifest = new_local_folder_manifest(self.local_author)
        manifest["children"][child_name] = child_access
        mark_manifest_modified(manifest)
        self._local_db.set(access, manifest)
        self._local_db.set(child_access, child_manifest)
        get_signal("fs.entry.modified").send("local", id=access["id"])
        get_signal("fs.entry.created").send("local", id=child_access["id"])

    def _delete(self, path, expect=None):
        path = normalize_path(path)
        if path == "/":
            raise PermissionError(13, "Permission denied", path)
        parent_path, child_name = path.rsplit("/", 1)
        parent_access, parent_manifest = self._retrieve_entry(parent_path or "/")
        if not is_folder_manifest(parent_manifest):
            raise NotADirectoryError(20, "Not a directory", parent_path)

        try:
            item_access = parent_manifest["children"].pop(child_name)
        except KeyError:
            raise FileNotFoundError(2, "No such file or directory", path)

        item_manifest = self._local_db.get(item_access)
        if is_folder_manifest(item_manifest):
            if expect == "file":
                raise IsADirectoryError(21, "Is a directory", path)
            if item_manifest["children"]:
                raise OSError(39, "Directory not empty", path)
        elif expect == "folder":
            raise NotADirectoryError(20, "Not a directory", path)

        self._local_db.set(parent_access, parent_manifest)
        get_signal("fs.entry.modified").send("local", id=parent_access["id"])

    def delete(self, path):
        self._delete(path)

    def unlink(self, path):
        self._delete(path, expect="file")

    def rmdir(self, path):
        self._delete(path, expect="folder")

    def move(self, src, dst):
        src = normalize_path(src)
        dst = normalize_path(dst)

        if src == dst:
            return

        parent_src, child_src = src.rsplit("/", 1)
        parent_dst, child_dst = dst.rsplit("/", 1)
        parent_src = parent_src or "/"
        parent_dst = parent_dst or "/"

        if parent_src == parent_dst:
            parent_access, parent_manifest = self._retrieve_entry(parent_src)
            if not is_folder_manifest(parent_manifest):
                raise NotADirectoryError(20, "Not a directory", parent_src)

            if dst.startswith(src + "/"):
                raise OSError(22, "Invalid argument", src, None, dst)

            try:
                entry = parent_manifest["children"].pop(child_src)
            except KeyError:
                raise FileNotFoundError(2, "No such file or directory", src)

            existing_entry_access = parent_manifest["children"].get(child_dst)
            if existing_entry_access:
                existing_entry_manifest = self._local_db.get(existing_entry_access)
                if is_folder_manifest(existing_entry_manifest):
                    raise IsADirectoryError(21, "Is a directory")

            parent_manifest["children"][child_dst] = entry
            mark_manifest_modified(parent_manifest)

            self._local_db.set(parent_access, parent_manifest)
            get_signal("fs.entry.modified").send("local", id=parent_access["id"])

        else:
            parent_src_access, parent_src_manifest = self._retrieve_entry(parent_src)
            if not is_folder_manifest(parent_src_manifest):
                raise NotADirectoryError(20, "Not a directory", parent_src)

            parent_dst_access, parent_dst_manifest = self._retrieve_entry(parent_dst)
            if not is_folder_manifest(parent_dst_manifest):
                raise NotADirectoryError(20, "Not a directory", parent_dst)

            if dst.startswith(src + "/"):
                raise OSError(22, "Invalid argument", src, None, dst)

            try:
                entry = parent_src_manifest["children"].pop(child_src)
            except KeyError:
                raise FileNotFoundError(2, "No such file or directory", src)

            existing_entry_access = parent_dst_manifest["children"].get(child_dst)
            if existing_entry_access:
                existing_entry_manifest = self._local_db.get(existing_entry_access)
                if is_folder_manifest(existing_entry_manifest):
                    raise IsADirectoryError(21, "Is a directory")

            parent_dst_manifest["children"][child_dst] = entry

            mark_manifest_modified(parent_src_manifest)
            mark_manifest_modified(parent_dst_manifest)

            self._local_db.set(parent_src_access, parent_src_manifest)
            self._local_db.set(parent_dst_access, parent_dst_manifest)

            get_signal("fs.entry.modified").send("local", id=parent_src_access["id"])
            get_signal("fs.entry.modified").send("local", id=parent_dst_access["id"])
