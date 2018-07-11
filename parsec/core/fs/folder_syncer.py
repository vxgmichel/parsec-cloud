import os

from parsec.core.fs.data import (
    is_folder_manifest,
    local_to_remote_manifest,
    remote_to_local_manifest,
)
from parsec.core.schemas import dumps_manifest


class FolderSyncerMixin:
    async def _sync_folder_nolock(self, path, access, manifest, recursive, notify_beacons):
        assert is_folder_manifest(manifest)
        if recursive:
            print("sync folder, sync children", access["id"])
            for child_name, child_access in manifest["children"].items():
                print("sync folder, sync children", access["id"], child_name, child_access["id"])
                if isinstance(recursive, dict):
                    child_recursive = recursive.get(child_name, False)
                else:
                    child_recursive = recursive
                child_path = os.path.join(path, child_name)
                await self._sync_nolock(child_path, child_access, child_recursive, notify_beacons)
                print("sync folder, sync children done", access["id"], child_name)

        # If recursive=False, placeholder are stored in parent but not resolved...

        if not manifest["need_sync"]:
            # TODO: User manifest should always be loaded
            self.local_folder_fs.mark_outdated_manifest(access)
            self.signal_ns.signal("fs.entry.synced").send(None, path=path, id=access["id"])
            print("sync folder, oudating marked", access["id"])
            return

        remote_manifest = local_to_remote_manifest(manifest)
        remote_manifest["version"] += 1

        ciphered = self.encryption_manager.encrypt_with_secret_key(
            access["key"], dumps_manifest(remote_manifest)
        )
        if manifest["is_placeholder"]:
            print("sync folder, placeholder sync", access["id"])
            await self._backend_vlob_create(
                access["id"], access["rts"], access["wts"], ciphered, notify_beacons
            )
            print("sync folder, placeholder sync done", access["id"])
        else:
            print("sync folder, update sync", access["id"])
            await self._backend_vlob_update(
                access["id"], access["wts"], remote_manifest["version"], ciphered, notify_beacons
            )
            print("sync folder, update sync done", access["id"])

        # Fuck the merge...
        updated_manifest = remote_to_local_manifest(remote_manifest)
        self.local_folder_fs.set_manifest(access, updated_manifest)

        print(" ********************** send signal to ", id(self.signal_ns))
        self.signal_ns.signal("fs.entry.synced").send(None, path=path, id=access["id"])
