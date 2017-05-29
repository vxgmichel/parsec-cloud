from copy import deepcopy
import json
from functools import partial
from base64 import encodebytes, decodebytes
from datetime import datetime
import os

from parsec.backend.vlob_service import VlobNotFound
from parsec.core.buffers import BufferedUserVlob, BufferedVlob
from parsec.core.synchronizer import synchronizer_factory
from parsec.crypto import AESCipher, Crypto
from parsec.exceptions import SymCryptoError, UserManifestError, UserManifestNotFound
from parsec.tools import event_handler


class Manifest:

    def __init__(self,
                 backend_service,
                 core_service,
                 identity_service):
        self.backend_service = backend_service
        self.core_service = core_service
        self.identity_service = identity_service
        self.entries = {'/': None}
        self.dustbin = []
        self.original_manifest = {'entries': deepcopy(self.entries),
                                  'dustbin': deepcopy(self.dustbin),
                                  'versions': {}}
        self.handler = partial(event_handler, self.reload, reset=False)

    async def reload(self):
        raise NotImplementedError()

    async def is_dirty(self):
        current_manifest = json.loads(await self.dumps())
        diff = await self.diff(self.original_manifest, current_manifest)
        for category in diff.keys():
            for operation in diff[category].keys():
                if diff[category][operation]:
                    return True
        return False

    async def diff(self, old_manifest, new_manifest):
        diff = {}
        for category in new_manifest.keys():
            if category == 'dustbin':
                continue
            added = {}
            changed = {}
            removed = {}
            for key, value in new_manifest[category].items():
                try:
                    ori_value = old_manifest[category][key]
                    if ori_value != value:
                        changed[key] = (ori_value, value)
                except KeyError:
                    added[key] = value
            for key, value in old_manifest[category].items():
                try:
                    new_manifest[category][key]
                except KeyError:
                    removed[key] = value
            diff.update({category: {'added': added, 'changed': changed, 'removed': removed}})
        # Dustbin
        added = []
        removed = []
        for vlob in new_manifest['dustbin']:
            if vlob not in old_manifest['dustbin']:
                added.append(vlob)
        for vlob in old_manifest['dustbin']:
            if vlob not in new_manifest['dustbin']:
                removed.append(vlob)
        diff.update({'dustbin': {'added': added, 'removed': removed}})
        return diff

    async def patch(self, manifest, diff):
        new_manifest = deepcopy(manifest)
        for category in diff.keys():
            if category in ['dustbin', 'versions']:
                continue
            for path, entry in diff[category]['added'].items():
                if path in new_manifest[category] and new_manifest[category][path] != entry:
                    new_manifest[category][path + '-conflict'] = new_manifest[category][path]
                new_manifest[category][path] = entry
            for path, entries in diff[category]['changed'].items():
                old_entry, new_entry = entries
                if path in new_manifest[category]:
                    current_entry = new_manifest[category][path]
                    if current_entry not in [old_entry, new_entry]:
                        new_manifest[category][path + '-conflict'] = current_entry
                    new_manifest[category][path] = new_entry
                else:
                    new_manifest[category][path + '-deleted'] = new_entry
            for path, entry in diff[category]['removed'].items():
                if path in new_manifest[category]:
                    if new_manifest[category][path] != entry:
                        new_manifest[category][path + '-recreated'] = new_manifest[category][path]
                    del new_manifest[category][path]
        for entry in diff['dustbin']['added']:
            if entry not in new_manifest['dustbin']:
                new_manifest['dustbin'].append(entry)
        for entry in diff['dustbin']['removed']:
            if entry in new_manifest['dustbin']:
                new_manifest['dustbin'].remove(entry)
        return new_manifest

    async def get_vlob(self):
        # TODO less intrusive
        return {
            'id': self.buffered_vlob.id,
            'read_trust_seed': self.buffered_vlob.read_trust_seed,
            'write_trust_seed': self.buffered_vlob.write_trust_seed
        }

    # async def get_version(self):
    #     return self.version

    async def get_vlobs_versions(self):
        versions = {}
        for entry in list(self.entries.values()) + self.dustbin:
            if entry:
                try:
                    vlob = await self.backend_service.vlob_read(entry['id'], entry['read_trust_seed'])
                except VlobNotFound:
                    versions[entry['id']] = None
                else:
                    versions[entry['id']] = vlob['version']
        return versions

    async def dumps(self, original_manifest=False):
        if original_manifest:
            return json.dumps(self.original_manifest)
        else:
            return json.dumps({'entries': self.entries,
                               'dustbin': self.dustbin,
                               'versions': await self.get_vlobs_versions()})

    async def reload_vlob(self, vlob_id):
        # TODO invalidate old cache
        pass

    async def add_file(self, path, vlob):
        path = '/' + path.strip('/')
        parent_folder = os.path.dirname(path)
        if parent_folder not in self.entries:
            raise UserManifestNotFound('Destination Folder not found.')
        if path in self.entries:
            raise UserManifestError('already_exists', 'File already exists.')
        self.entries[path] = vlob

    async def rename_file(self, old_path, new_path):
        old_path = '/' + old_path.strip('/')
        new_path = '/' + new_path.strip('/')
        new_parent_folder = os.path.dirname(new_path)
        if new_parent_folder not in self.entries:
            raise UserManifestNotFound('Destination Folder not found.')
        if new_path in self.entries:
            raise UserManifestError('already_exists', 'File already exists.')
        if old_path not in self.entries:
            raise UserManifestNotFound('File not found.')
        for entry, vlob in self.entries.items():
            if entry.startswith(old_path):
                new_entry = new_path + entry[len(old_path):]
                self.entries[new_entry] = vlob
                del self.entries[entry]

    async def delete_file(self, path):
        path = '/' + path.strip('/')
        try:
            entry = self.entries[path]
        except KeyError:
            raise UserManifestNotFound('File not found.')
        if not entry['id']:
            raise UserManifestError('path_is_not_file', 'Path is not a file.')
        dustbin_entry = {'removed_date': datetime.utcnow().timestamp(), 'path': path}
        dustbin_entry.update(entry)
        self.dustbin.append(dustbin_entry)
        del self.entries[path]

    async def undelete_file(self, vlob):
        for entry in self.dustbin:
            if entry['id'] == vlob:
                path = entry['path']
                if path in self.entries:
                    raise UserManifestError('already_exists', 'Restore path already used.')
                del entry['path']
                del entry['removed_date']
                self.dustbin[:] = [item for item in self.dustbin if item['id'] != vlob]
                self.entries[path] = entry
                folder = os.path.dirname(path)
                await self.make_folder(folder, parents=True)
                return
        raise UserManifestNotFound('Vlob not found.')

    async def reencrypt_file(self, path):
        path = '/' + path.strip('/')
        try:
            entry = self.entries[path]
        except KeyError:
            raise UserManifestNotFound('File not found.')
        new_vlob = await self.core_service.reencrypt(entry['id'])
        self.entries[path] = new_vlob

    async def path_info(self, path, children=True):
        path = '/' + path.strip('/')
        if path != '/' and path not in self.entries:
            raise UserManifestNotFound('Folder or file not found.')
        if not children:
            return deepcopy(self.entries[path])
        results = {}
        for entry in self.entries:
            if entry != path and entry.startswith(path) and entry.count('/', len(path) + 1) == 0:
                results[os.path.basename(entry)] = deepcopy(self.entries[entry])
        return results

    async def make_folder(self, path, parents=False):
        path = '/' + path.strip('/')
        if path in self.entries:
            if parents:
                return self.entries[path]
            else:
                raise UserManifestError('already_exists', 'Folder already exists.')
        parent_folder = os.path.dirname(path)
        if parent_folder not in self.entries:
            if parents:
                await self.make_folder(parent_folder, parents=True)
            else:
                raise UserManifestNotFound("Parent folder doesn't exists.")
        self.entries[path] = None
        return self.entries[path]

    async def remove_folder(self, path):
        path = '/' + path.strip('/')
        if path == '/':
            raise UserManifestError('cannot_remove_root', 'Cannot remove root folder.')
        for entry, vlob in self.entries.items():
            if entry != path and entry.startswith(path):
                raise UserManifestError('folder_not_empty', 'Folder not empty.')
            elif entry == path and vlob['id']:
                raise UserManifestError('path_is_not_folder', 'Path is not a folder.')
        try:
            del self.entries[path]
        except KeyError:
            raise UserManifestNotFound('Folder not found.')

    async def show_dustbin(self, path=None):
        if not path:
            return self.dustbin
        else:
            path = '/' + path.strip('/')
        results = [entry for entry in self.dustbin if entry['path'] == path]
        if not results:
            raise UserManifestNotFound('Path not found.')
        return results

    async def check_consistency(self, manifest):
        entries = [entry for entry in list(manifest['entries'].values()) if entry]
        entries += manifest['dustbin']
        for entry in entries:
            try:
                vlob = await self.backend_service.vlob_read(
                    id=entry['id'],
                    trust_seed=entry['read_trust_seed'],
                    version=manifest['versions'][entry['id']])
                encrypted_blob = vlob['blob']
                encrypted_blob = decodebytes(encrypted_blob.encode())
                key = decodebytes(entry['key'].encode()) if entry['key'] else None
                encryptor = AESCipher()
                encryptor.decrypt(key, encrypted_blob)
            except (VlobNotFound, SymCryptoError):
                return False
        return True


class GroupManifest(Manifest):

    def __init__(self,
                 backend_service,
                 core_service,
                 identity_service,
                 buffered_vlob,
                 key=None):
        super().__init__(backend_service,
                         core_service,
                         identity_service)
        self.buffered_vlob = buffered_vlob

    # async def update_vlob(self, new_vlob):
    #     self.buffered_vlob = new_vlob  # TODO create vlob object from dict?

    async def diff_versions(self, old_version=None, new_version=None):
        empty_entries = {'/': None}
        empty_manifest = {'entries': empty_entries, 'dustbin': [], 'versions': {}}
        # Old manifest
        encryptor = AESCipher()
        if old_version and old_version > 0:
            old_vlob = await self.backend_service.read(
                id=self.id,
                trust_seed=self.read_trust_seed,
                version=old_version)
            key = decodebytes(self.key.encode())
            content = encryptor.decrypt(key, old_vlob['blob'])
            old_manifest = json.loads(content.decode())
        elif old_version == 0:
            old_manifest = empty_manifest
        else:
            old_manifest = self.original_manifest
        # New manifest
        if new_version and new_version > 0:
            new_vlob = await self.backend_service.vlob_read(
                id=self.id,
                trust_seed=self.read_trust_seed,
                version=new_version)
            key = decodebytes(self.key.encode())
            content = encryptor.decrypt(key, new_vlob['blob'])
            new_manifest = json.loads(content.decode())
        elif new_version == 0:
            new_manifest = empty_manifest
        else:
            new_manifest = json.loads(await self.dumps())
        return await self.diff(old_manifest, new_manifest)

    async def reload(self, reset=False):
        if not self.id:
            raise UserManifestError('missing_id', 'Group manifest has no ID.')
        # Subscribe to events
        await self.backend_service.connect_event('on_vlob_updated', self.id, self.handler)
        try:
            vlob_properties = await self.buffered_vlob.get_vlob()
            vlob = await self.backend.vlob_read(vlob_properties['id'],
                                                vlob_properties['read_trust_seed'])
        except VlobNotFound:
            raise UserManifestNotFound('Group manifest not found.')
        key = decodebytes(self.key.encode())
        encryptor = AESCipher()
        content = encryptor.decrypt(key, vlob['blob'])
        if not reset and vlob['version'] <= await self.buffered_vlob.get_version():
            return
        new_manifest = json.loads(content.decode())
        backup_new_manifest = deepcopy(new_manifest)
        if not await self.check_consistency(new_manifest):
            raise UserManifestError('not_consistent', 'Group manifest not consistent.')
        if not reset:
            diff = await self.diff_versions()
            new_manifest = await self.patch(new_manifest, diff)
        self.entries = new_manifest['entries']
        self.dustbin = new_manifest['dustbin']
        await self.buffered_vlob.set_version(vlob['version'])
        self.original_manifest = backup_new_manifest
        versions = new_manifest['versions']
        for vlob_id, version in versions.items():
            await self.core_service.file_restore(vlob_id, version)

    async def commit(self):
        if not await self.is_dirty():
            return
        blob = await self.dumps()
        if self.key:
            key = key = decodebytes(self.key.encode())
        else:
            key = None
        encryptor = AESCipher()
        key, encrypted_blob = encryptor.encrypt(key, blob.encode())
        await self.buffered_vlob.update(encrypted_blob.decode())
        self.original_manifest = json.loads(blob)
        synchonizer = synchronizer_factory()
        await synchonizer.set_synchronizable_flag(self.buffered_vlob, True)

    async def reencrypt(self):
        # Reencrypt files
        for path, entry in self.entries.items():
            if entry:
                new_vlob = await self.core_service.file_reencrypt(entry['id'])
                self.entries[path] = new_vlob
        for index, entry in enumerate(self.dustbin):
            path = entry['path']
            removed_date = entry['removed_date']
            new_vlob = await self.core_service.file_reencrypt(entry['id'])
            new_vlob['path'] = path
            new_vlob['removed_date'] = removed_date
            self.dustbin[index] = new_vlob
        # Reencrypt manifest
        blob = await self.dumps()
        encryptor = AESCipher()
        key, encrypted_blob = encryptor.encrypt(blob.encode())
        self.buffered_vlob = BufferedVlob.create(self.backend_service, blob=encrypted_blob.decode())

    async def restore(self, version=None):
        current_version = await self.buffered_vlob.get_version()
        if version is None:
            version = current_version - 1 if current_version > 1 else 1
        if version > 0 and version < current_version:
            vlob_properties = await self.buffered_vlob.get_vlob()
            vlob = await self.backend.vlob_read(vlob_properties['id'],
                                                vlob_properties['read_trust_seed'],
                                                vlob_properties['version'])
            await self.buffered_vlob.update(vlob['blob'])
        elif version < 1 or version > current_version:
            raise UserManifestError('bad_version', 'Bad version number.')
        await self.reload(reset=True)


class UserManifest(Manifest):

    def __init__(self,
                 backend_service,
                 core_service,
                 identity_service,
                 buffered_user_vlob):
        super().__init__(backend_service,
                         core_service,
                         identity_service)
        self.buffered_user_vlob = buffered_user_vlob
        self.group_manifests = {}
        self.original_manifest = {'entries': deepcopy(self.entries),
                                  'dustbin': deepcopy(self.dustbin),
                                  'groups': deepcopy(self.group_manifests),
                                  'versions': {}}

    async def diff_versions(self, old_version=None, new_version=None):
        empty_entries = {'/': None}
        empty_manifest = {'entries': empty_entries, 'groups': {}, 'dustbin': [], 'versions': {}}
        crypto = Crypto(AESCipher(), self.identity_service.private_key)
        # Old manifest
        if old_version and old_version > 0:
            old_vlob = await self.backend_service.user_vlob_read(old_version)
            old_blob = json.loads(old_vlob['blob'])
            content = await crypto.decrypt(**old_blob)
            old_manifest = json.loads(content.decode())
        elif old_version == 0:
            old_manifest = empty_manifest
        else:
            old_manifest = self.original_manifest
        # New manifest
        if new_version and new_version > 0:
            new_vlob = await self.backend_service.user_vlob_read(new_version)
            new_blob = json.loads(new_vlob['blob'])
            content = await crypto.decrypt(**new_blob)
            new_manifest = json.loads(content.decode())
        elif new_version == 0:
            new_manifest = empty_manifest
        else:
            new_manifest = json.loads(await self.dumps())
        return await self.diff(old_manifest, new_manifest)

    async def dumps(self, original_manifest=False):
        if original_manifest:
            return json.dumps(self.original_manifest)
        else:
            return json.dumps({'entries': self.entries,
                               'dustbin': self.dustbin,
                               'groups': await self.get_group_vlobs(),
                               'versions': await self.get_vlobs_versions()})

    async def get_group_vlobs(self, group=None):
        if group:
            groups = [group]
        else:
            groups = self.group_manifests.keys()
        results = {}
        try:
            for group in groups:
                results[group] = await self.group_manifests[group].get_vlob()
        except KeyError:
            raise UserManifestNotFound('Group not found.')
        return results

    async def get_group_manifest(self, group):
        try:
            return self.group_manifests[group]
        except KeyError:
            raise UserManifestNotFound('Group not found.')

    async def reencrypt_group_manifest(self, group):
        try:
            group_manifest = self.group_manifests[group]
        except KeyError:
            raise UserManifestNotFound('Group not found.')
        await group_manifest.reencrypt()

    async def create_group_manifest(self, group):
        if group in self.group_manifests:
            raise UserManifestError('already_exists', 'Group already exists.')
        buffered_vlob = await BufferedVlob.create(self.backend_service)
        group_manifest = GroupManifest(self.backend_service,
                                       self.core_service,
                                       self.identity_service,
                                       buffered_vlob)
        self.group_manifests[group] = group_manifest

    async def import_group_vlob(self, group, vlob):
        if group in self.group_manifests:
            await self.group_manifests[group].update_vlob(vlob)
            await self.group_manifests[group].reload(reset=False)
        group_manifest = GroupManifest(self.backend_service,
                                       self.core_service,
                                       self.identity_service,
                                       self.backend_service,
                                       self.backend_service,
                                       **vlob)
        await group_manifest.reload(reset=True)
        self.group_manifests[group] = group_manifest

    async def remove_group(self, group):
        # TODO deleted group is not moved in dusbin, but hackers could continue to read/write files
        try:
            del self.group_manifests[group]
        except KeyError:
            raise UserManifestNotFound('Group not found.')

    async def reload(self, reset=False):
        # TODO: Named vlob should use private key handshake instead of trust_seed
        vlob = await self.backend_service.user_vlob_read()
        if vlob['blob'] == '':
            raise UserManifestNotFound('User manifest not found.')
        crypto = Crypto(AESCipher(), self.identity_service.private_key)
        blob = json.loads(vlob['blob'])
        content = await crypto.decrypt(**blob)
        if not reset and vlob['version'] <= await self.buffered_user_vlob.get_version():
            return
        new_manifest = json.loads(content.decode())
        backup_new_manifest = deepcopy(new_manifest)
        if not await self.check_consistency(new_manifest):
            raise UserManifestError('not_consistent', 'User manifest not consistent.')
        if not reset:
            diff = await self.diff_versions()
            new_manifest = await self.patch(new_manifest, diff)
        self.entries = new_manifest['entries']
        self.dustbin = new_manifest['dustbin']
        await self.buffered_user_vlob.set_version(vlob['version'])
        for group, group_vlob in new_manifest['groups'].items():
            await self.import_group_vlob(group, group_vlob)
        self.original_manifest = backup_new_manifest
        versions = new_manifest['versions']
        for vlob_id, version in versions.items():
            await self.core_service.file_restore(vlob_id, version)
        # Update event subscriptions
        # TODO update events subscriptions
        # Subscribe to events
        # TODO where to unsubscribe?

    async def commit(self, recursive=True):
        if await self.buffered_user_vlob.get_version() and not await self.is_dirty():
            return
        if recursive:
            for group_manifest in self.group_manifests.values():
                await group_manifest.commit()
        blob = await self.dumps()
        crypto = Crypto(AESCipher(), self.identity_service.private_key)
        encrypted_blob = await crypto.encrypt(blob.encode())
        encrypted_blob = json.dumps(encrypted_blob)
        await self.buffered_user_vlob.update(encrypted_blob)
        self.original_manifest = json.loads(blob)
        synchonizer = synchronizer_factory()
        await synchonizer.set_synchronizable_flag(self.buffered_user_vlob, True)

    async def restore(self, version=None):
        current_version = await self.buffered_user_vlob.get_version()
        if version is None:
            version = current_version - 1 if current_version > 1 else 1
        if version > 0 and version < current_version:
            vlob = await self.backend_service.user_vlob_read(version)
            await self.backend_service.update(current_version, vlob['blob'])
            await self.backend_service.flush()
        elif version < 1 or version > current_version:
            raise UserManifestError('bad_version', 'Bad version number.')
        await self.reload(reset=True)

    async def check_consistency(self, manifest):
        if await super().check_consistency(manifest) is False:
            return False
        encryptor = AESCipher()
        for group_manifest in self.group_manifests.values():
            entry = await group_manifest.get_vlob()
            try:
                vlob = await self.backend_service.read(
                    id=entry['id'],
                    trust_seed=entry['read_trust_seed'])
                encrypted_blob = vlob['blob']
                key = decodebytes(entry['key'].encode()) if entry['key'] else None
                encryptor.decrypt(key, encrypted_blob)
            except (VlobNotFound, SymCryptoError):
                return False
        return True
