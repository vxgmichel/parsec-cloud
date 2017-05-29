from base64 import encodebytes, decodebytes
from datetime import datetime
import json
from marshmallow import fields

from parsec.exceptions import FileError, FileNotFound, UserManifestError, UserManifestNotFound
from parsec.core.file import File
from parsec.crypto import AESCipher
from parsec.service import BaseService, cmd, event
from parsec.tools import BaseCmdSchema


class cmd_CREATE_FILE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    content = fields.String(missing='')
    group = fields.String(missing=None)


class cmd_RENAME_FILE_Schema(BaseCmdSchema):
    old_path = fields.String(required=True)
    new_path = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_DELETE_FILE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_UNDELETE_FILE_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_STAT_FILE_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    version = fields.Integer(missing=None, validate=lambda n: n >= 1)


class cmd_HISTORY_FILE_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    first_version = fields.Integer(missing=1, validate=lambda n: n >= 1)
    last_version = fields.Integer(missing=None, validate=lambda n: n >= 1)


class cmd_RESTORE_FILE_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    version = fields.Integer(missing=None, validate=lambda n: n >= 1)
    group = fields.String(missing=None)


class cmd_REENCRYPT_FILE_Schema(BaseCmdSchema):
    id = fields.String(required=True)


class FileManagementAPIMixin(BaseService):

    on_file_changed = event('file_changed')

    @cmd('file_create')
    async def _cmd_CREATE_FILE(self, session, msg):
        msg = cmd_CREATE_FILE_Schema().load(msg)
        file = await self.file_create(msg['path'], msg['content'], msg['group'])
        return {'status': 'ok', **file}

    @cmd('file_rename')
    async def _cmd_RENAME_FILE(self, session, msg):
        msg = cmd_RENAME_FILE_Schema().load(msg)
        await self.file_rename(msg['old_path'], msg['new_path'], msg['group'])
        return {'status': 'ok'}

    @cmd('file_delete')
    async def _cmd_DELETE_FILE(self, session, msg):
        msg = cmd_DELETE_FILE_Schema().load(msg)
        await self.file_delete(msg['path'], msg['group'])
        return {'status': 'ok'}

    @cmd('file_undelete')
    async def _cmd_UNDELETE_FILE(self, session, msg):
        msg = cmd_UNDELETE_FILE_Schema().load(msg)
        await self.file_undelete(msg['id'], msg['group'])
        return {'status': 'ok'}

    @cmd('file_stat')
    async def _cmd_STAT(self, session, msg):
        msg = cmd_STAT_FILE_Schema().load(msg)
        stat = await self.file_stat(msg['id'], msg['version'])
        stat.update({'status': 'ok'})
        return stat

    @cmd('file_history')
    async def _cmd_HISTORY(self, session, msg):
        msg = cmd_HISTORY_FILE_Schema().load(msg)
        history = await self.file_history(msg['id'], msg['first_version'], msg['last_version'])
        return {'status': 'ok', 'history': history}

    @cmd('file_restore')
    async def _cmd_RESTORE(self, session, msg):
        msg = cmd_RESTORE_FILE_Schema().load(msg)
        await self.file_restore(msg['id'], msg['version'])
        return {'status': 'ok'}

    @cmd('file_reencrypt')
    async def _cmd_REENCRYPT(self, session, msg):
        msg = cmd_REENCRYPT_FILE_Schema().load(msg)
        file = await self.file_reencrypt(msg['id'])
        file.update({'status': 'ok'})
        return file

    async def file_create(self, path, content=b'', group=None):
        manifest = await self.get_manifest(group)
        try:
            await manifest.path_info(path, children=False)
        except UserManifestNotFound:
            file = await File.create(self.backend, self.block, '')
            vlob = await file.get_vlob()
            # vlob = await self.buffered_vlob.create('')
            # blob = [await self._build_file_blocks(content, vlob['id'])]
            # # Encrypt blob
            # blob = json.dumps(blob)
            # blob = blob.encode()
            # encryptor = AESCipher()
            # blob_key, encrypted_blob = encryptor.encrypt(blob)
            # encrypted_blob = encodebytes(encrypted_blob).decode()
            # await self.buffered_vlob.update(vlob['id'], 1, encrypted_blob, vlob['write_trust_seed'])
            # del vlob['status']
            # vlob['key'] = encodebytes(blob_key).decode()
            await manifest.add_file(path, vlob)
            await manifest.commit()
            return vlob
        else:
            raise UserManifestError('already_exists', 'File already exists.')

    async def file_rename(self, old_path, new_path, group=None):
        manifest = await self.get_manifest(group)
        await manifest.rename_file(old_path, new_path)
        await manifest.commit()

    async def file_delete(self, path, group=None):
        manifest = await self.get_manifest(group)
        await manifest.delete_file(path)
        await manifest.commit()

    async def file_undelete(self, vlob, group=None):
        manifest = await self.get_manifest(group)
        await manifest.undelete_file(vlob)
        await manifest.commit()

    async def file_stat(self, id, version=None):
        try:
            properties = await self.get_properties(id=id)
        except FileNotFound:
            properties = await self.get_properties(id=id, dustbin=True)
        vlob = await self.backend.vlob_read(id, properties['read_trust_seed'], version)
        encrypted_blob = vlob['blob']
        encrypted_blob = decodebytes(encrypted_blob.encode())
        key = decodebytes(properties['key'].encode())
        encryptor = AESCipher()
        blob = encryptor.decrypt(key, encrypted_blob)
        blob = json.loads(blob.decode())
        # TODO which block index? Or add timestamp in vlob_service ?
        stat = await self.block.stat(blob[-1]['blocks'][-1]['block'])
        size = 0
        for blocks_and_key in blob:
            for block in blocks_and_key['blocks']:
                size += block['size']
        # TODO: don't provide atime field if we don't know it?
        return {'id': id,
                'ctime': stat['creation_timestamp'],
                'mtime': stat['creation_timestamp'],
                'atime': stat['creation_timestamp'],
                'size': size,
                'version': vlob['version']}

    async def file_history(self, id, first_version, last_version):
        if first_version and last_version and first_version > last_version:
            raise FileError('bad_versions',
                            'First version number higher than the second one.')
        history = []
        if not last_version:
            stat = await self.file_stat(id)
            last_version = stat['version']
        for current_version in range(first_version, last_version + 1):
            stat = await self.file_stat(id, current_version)
            del stat['id']
            history.append(stat)
        return history

    async def file_restore(self, id, version=None):
        try:
            properties = await self.get_properties(id=id)
        except FileNotFound:
            try:
                properties = await self.get_properties(id=id, dustbin=True)
            except FileNotFound:
                raise FileNotFound('Vlob not found.')
        stat = await self.file_stat(id)
        if version is None:
            version = stat['version'] - 1 if stat['version'] > 1 else 1
        if version > 0 and version < stat['version']:
            vlob = await self.buffered_vlob.read(
                id,
                properties['read_trust_seed'],
                version)
            await self.buffered_vlob.update(
                vlob_id=id,
                version=stat['version'],
                blob=vlob['blob'],
                trust_seed=properties['write_trust_seed'])
        elif version < 1 or version > stat['version']:
            raise FileError('bad_version', 'Bad version number.')

    async def file_reencrypt(self, id):
        try:
            properties = await self.get_properties(id=id)
        except FileNotFound:
            try:
                properties = await self.get_properties(id=id, dustbin=True)
            except FileNotFound:
                raise FileNotFound('Vlob not found.')
        old_vlob = await self.buffered_vlob.read(properties['id'], properties['read_trust_seed'])
        old_blob = old_vlob['blob']
        old_encrypted_blob = decodebytes(old_blob.encode())
        old_blob_key = decodebytes(properties['key'].encode())
        encryptor = AESCipher()
        new_blob = encryptor.decrypt(old_blob_key, old_encrypted_blob)
        new_key, new_encrypted_blob = encryptor.encrypt(new_blob)
        new_encrypted_blob = encodebytes(new_encrypted_blob).decode()
        new_key = encodebytes(new_key).decode()
        new_vlob = await self.buffered_vlob.create(new_encrypted_blob)
        del new_vlob['status']
        new_vlob['key'] = new_key
        return new_vlob
