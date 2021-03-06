from typing import Optional, List
from structlog import get_logger

from parsec.core.fs.types import Access, Path, LocalFolderManifest, RemoteFolderManifest
from parsec.core.fs.utils import (
    is_user_manifest,
    is_folder_manifest,
    is_placeholder_manifest,
    local_to_remote_manifest,
    remote_to_local_manifest,
)
from parsec.core.fs.sync_base import SyncConcurrencyError, BaseSyncer
from parsec.core.fs.merge_folders import merge_local_folder_manifests, merge_remote_folder_manifests
from parsec.core.fs.local_folder_fs import FSManifestLocalMiss


logger = get_logger()


class FolderSyncerMixin(BaseSyncer):
    async def _sync_folder_look_for_remote_changes(
        self, access: Access, manifest: LocalFolderManifest
    ) -> Optional[RemoteFolderManifest]:
        # Placeholder means we need synchro !
        assert not is_placeholder_manifest(manifest)
        # This folder hasn't been modified locally, just download
        # last version from the backend if any.
        target_remote_manifest = await self._backend_vlob_read(access)
        if target_remote_manifest["version"] == manifest["base_version"]:
            return None
        return target_remote_manifest

    def _strip_placeholders(self, children: List[Access]):
        synced_children = {}
        for child_name, child_access in children.items():
            try:
                child_manifest = self.local_folder_fs.get_manifest(child_access)
            except FSManifestLocalMiss:
                # Child not in local, cannot be a placeholder then !
                synced_children[child_name] = child_access
            else:
                if not is_placeholder_manifest(child_manifest):
                    synced_children[child_name] = child_access
        return synced_children

    async def _sync_folder_actual_sync(
        self, path: Path, access: Access, manifest: LocalFolderManifest
    ) -> RemoteFolderManifest:
        to_sync_manifest = local_to_remote_manifest(manifest)
        to_sync_manifest["version"] += 1

        # Upload the folder manifest as new vlob version
        notify_beacons = self.local_folder_fs.get_beacon(path)
        force_update = False
        while True:
            try:
                if is_placeholder_manifest(manifest) and not force_update:
                    await self._backend_vlob_create(access, to_sync_manifest, notify_beacons)
                else:
                    await self._backend_vlob_update(access, to_sync_manifest, notify_beacons)
                break

            except SyncConcurrencyError:
                if is_placeholder_manifest(manifest):
                    # Placeholder don't have remote version, so concurrency shouldn't
                    # be possible. However special cases exist:
                    # - user manifest has it access is shared between devices
                    #   even if it is not yet synced.
                    # - it's possible a previous attempt of uploading this
                    #   manifest succeeded but we didn't receive the backend's
                    #   answer, hence wrongly believing this is still a placeholder.
                    # If such cases occured, we just have to pretend we were
                    # trying to do an update and rely on the generic merge.

                    if is_user_manifest(manifest):
                        logger.warning(
                            "Concurrency error while creating user vlob", access_id=access["id"]
                        )
                    else:
                        logger.warning(
                            "Concurrency error while creating vlob", access_id=access["id"]
                        )

                    base = None
                    force_update = True
                else:
                    base = await self._backend_vlob_read(access, to_sync_manifest["version"] - 1)

                # Do a 3-ways merge to fix the concurrency error, first we must
                # fetch the base version and the new one present in the backend
                # TODO: base should be available locally
                target = await self._backend_vlob_read(access)

                # 3-ways merge between base, modified and target versions
                to_sync_manifest, sync_needed, conflicts = merge_remote_folder_manifests(
                    base, to_sync_manifest, target
                )
                for original_name, original_id, diverged_name, diverged_id in conflicts:
                    self.event_bus.send(
                        "fs.entry.name_conflicted",
                        path=str(path / original_name),
                        diverged_path=str(path / diverged_name),
                        original_id=original_id,
                        diverged_id=diverged_id,
                    )
                if not sync_needed:
                    # It maybe possible the changes that cause the concurrency
                    # error were the same than the one we wanted to make in the
                    # first place (e.g. when removing the same file)
                    break
                to_sync_manifest["version"] = target["version"] + 1

        return to_sync_manifest

    async def _sync_folder(
        self, path: Path, access: Access, manifest: LocalFolderManifest, recursive: bool
    ) -> None:
        assert not is_placeholder_manifest(manifest)
        assert is_folder_manifest(manifest)

        # Synchronizing a folder is divided into three steps:
        # - first synchronizing it children
        # - then sychronize itself
        # - finally merge the synchronized version with the current one (that
        #   may have been updated in the meantime)

        # Synchronizing children
        if recursive:
            for child_name, child_access in sorted(
                manifest["children"].items(), key=lambda x: x[0]
            ):
                child_path = path / child_name
                try:
                    await self._sync_nolock(child_path, True)
                except FileNotFoundError:
                    # Concurrent deletion occured, just ignore this child
                    pass

            # The trick here is to retreive the current version of the manifest
            # and remove it placeholders (those are the children created since
            # the start of our sync)
            manifest = self.local_folder_fs.get_manifest(access)
            assert is_folder_manifest(manifest)

        manifest["children"] = self._strip_placeholders(manifest["children"])

        # Now we can synchronize the folder if needed
        if not manifest["need_sync"]:
            target_remote_manifest = await self._sync_folder_look_for_remote_changes(
                access, manifest
            )
            # Quick exit if nothing's new
            if not target_remote_manifest:
                return
            event_type = "fs.entry.remote_changed"
        else:
            target_remote_manifest = await self._sync_folder_actual_sync(path, access, manifest)
            event_type = "fs.entry.synced"
        assert is_folder_manifest(target_remote_manifest)

        # Merge the synchronized version with the current one
        self._sync_folder_merge_back(path, access, manifest, target_remote_manifest)

        self.event_bus.send(event_type, path=str(path), id=access["id"])

    async def _minimal_sync_folder(
        self, path: Path, access: Access, manifest: LocalFolderManifest
    ) -> bool:
        """
        Returns: If additional sync are needed
        Raises:
            FileSyncConcurrencyError
            BackendNotAvailable
        """
        if not is_placeholder_manifest(manifest):
            return manifest["need_sync"]

        synced_children = self._strip_placeholders(manifest["children"])
        need_more_sync = synced_children.keys() != manifest["children"].keys()
        manifest["children"] = synced_children

        target_remote_manifest = await self._sync_folder_actual_sync(path, access, manifest)
        self._sync_folder_merge_back(path, access, manifest, target_remote_manifest)

        self.event_bus.send("fs.entry.minimal_synced", path=str(path), id=access["id"])
        return need_more_sync

    def _sync_folder_merge_back(
        self,
        path: Path,
        access: Access,
        base_manifest: LocalFolderManifest,
        target_remote_manifest: RemoteFolderManifest,
    ) -> None:
        # Merge with the current version of the manifest which may have
        # been modified in the meantime
        assert is_folder_manifest(target_remote_manifest)
        current_manifest = self.local_folder_fs.get_manifest(access)
        assert is_folder_manifest(current_manifest)

        target_manifest = remote_to_local_manifest(target_remote_manifest)
        final_manifest, conflicts = merge_local_folder_manifests(
            base_manifest, current_manifest, target_manifest
        )
        for original_name, original_id, diverged_name, diverged_id in conflicts:
            self.event_bus.send(
                "fs.entry.name_conflicted",
                path=str(path / original_name),
                diverged_path=str(path / diverged_name),
                original_id=original_id,
                diverged_id=diverged_id,
            )
        self.local_folder_fs.set_manifest(access, final_manifest)
