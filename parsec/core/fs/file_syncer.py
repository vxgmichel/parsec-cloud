from hashlib import sha256

from parsec.core.fs.data import is_file_manifest, remote_to_local_manifest
from parsec.core.schemas import dumps_manifest


class FileSyncerMixin:
    async def _sync_file_nolock(self, path, access, manifest, notify_beacons):
        assert is_file_manifest(manifest)
        if not manifest["need_sync"]:
            self.local_folder_fs.mark_outdated_manifest(access)
            self.signal_ns.signal("fs.entry.synced").send(None, path=path, id=access["id"])
            print("sync file oudating entry", access["id"])
            return

        print("sync file uploading blocks", access["id"])
        for db_access in manifest["dirty_blocks"]:
            db = self.local_file_fs.get_block(db_access)
            db_access["digest"] = sha256(db).hexdigest()
            await self._backend_block_post(db_access, db)
        manifest["blocks"] += manifest["dirty_blocks"]
        print("sync file blocks uploaded", access["id"])

        remote_manifest = {
            "type": "file_manifest",
            "version": manifest["base_version"] + 1,
            "blocks": manifest["blocks"] + manifest["dirty_blocks"],
            "created": manifest["created"],
            "updated": manifest["updated"],
            "size": manifest["size"],
            "author": self.device.id,
        }

        if manifest["is_placeholder"]:
            print("sync file placeholder sync", access["id"])
            await self._backend_vlob_create(access, remote_manifest, notify_beacons)
            print("sync file placeholder sync done", access["id"])
        else:
            print("sync file update sync", access["id"])
            await self._backend_vlob_update(access, remote_manifest, notify_beacons)
            print("sync file update sync done", access["id"])

        # Fuck the merge...
        updated_manifest = remote_to_local_manifest(remote_manifest)
        self.local_folder_fs.set_manifest(access, updated_manifest)

        print(" ********************** send signal to ", id(self.signal_ns))
        self.signal_ns.signal("fs.entry.synced").send(None, path=path, id=access["id"])
