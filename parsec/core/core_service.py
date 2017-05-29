from copy import deepcopy

from parsec.service import BaseService, service
from parsec.core.buffers import BufferedUserVlob
from parsec.core.file_management_api import FileManagementAPIMixin
from parsec.core.file_operations_api import FileOperationsAPIMixin
from parsec.core.folder_api import FolderAPIMixin
from parsec.core.manifest_api import ManifestAPIMixin
from parsec.core.manifest import UserManifest
from parsec.core.share_api import ShareAPIMixin
from parsec.exceptions import FileNotFound, UserManifestError, UserManifestNotFound, VlobNotFound


class CoreService(FileManagementAPIMixin,
                  FileOperationsAPIMixin,
                  FolderAPIMixin,
                  ManifestAPIMixin,
                  ShareAPIMixin,
                  BaseService):

    name = 'CoreService'

    backend = service('BackendAPIService')
    block = service('BlockService')
    identity = service('IdentityService')

    def __init__(self):
        super().__init__()
        self.user_manifest = None

    # TODO move these methods in others modules

    async def reencrypt_group_manifest(self, group):
        manifest = await self.get_manifest()
        await manifest.reencrypt_group_manifest(group)
        await manifest.commit()

    async def import_group_vlob(self, group, vlob):
        manifest = await self.get_manifest()
        await manifest.import_group_vlob(group, vlob)
        await manifest.commit()

    async def import_file_vlob(self, path, vlob, group=None):
        manifest = await self.get_manifest(group)
        await manifest.add_file(path, vlob)
        await manifest.commit()

    async def restore_file(self, vlob, group=None):
        manifest = await self.get_manifest(group)
        await manifest.restore_file(vlob)

    async def load_user_manifest(self):
        identity = self.identity.id
        if not self.user_manifest or (await self.user_manifest.get_vlob())['id'] != identity:
            user_vlob = await self.backend.user_vlob_read()
            buffered_user_vlob = await BufferedUserVlob.create(self.backend, user_vlob['version'])
            self.user_manifest = UserManifest(self.backend,
                                              self,
                                              self.identity,
                                              buffered_user_vlob)
        try:
            await self.user_manifest.reload(False)
        except (UserManifestError, VlobNotFound):
            await self.user_manifest.commit()
            await self.user_manifest.reload(True)

    async def get_manifest(self, group=None):
        if not self.user_manifest:
            await self.load_user_manifest()
        if group:
            return await self.user_manifest.get_group_manifest(group)
        else:
            return self.user_manifest

    async def get_properties(self, path=None, id=None, dustbin=False, group=None):  # TODO refactor?
        if group and not id and not path:
            manifest = await self.get_manifest(group)
            return await manifest.get_vlob()
        groups = [group] if group else [None] + list(await self.user_manifest.get_group_vlobs())
        for current_group in groups:
            manifest = await self.get_manifest(current_group)
            if dustbin:
                for item in manifest.dustbin:
                    if path == item['path'] or id == item['id']:
                        return deepcopy(item)
            else:
                if path in manifest.entries:
                    return deepcopy(manifest.entries[path])
                elif id:
                    for entry in manifest.entries.values():  # TODO bad complexity
                        if entry and entry['id'] == id:
                            return deepcopy(entry)
        raise FileNotFound('File not found.')
