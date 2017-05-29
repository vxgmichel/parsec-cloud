from base64 import decodebytes, encodebytes
import json
import sys

from cryptography.hazmat.backends.openssl import backend as openssl
from cryptography.hazmat.primitives import hashes
from marshmallow import fields

from parsec.core.file import File
from parsec.crypto import AESCipher
from parsec.exceptions import FileNotFound
from parsec.service import BaseService, cmd, event
from parsec.tools import BaseCmdSchema


class cmd_FILE_OPEN_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_FILE_READ_Schema(BaseCmdSchema):
    fd = fields.Int(required=True)
    version = fields.Integer(missing=None, validate=lambda n: n >= 1)
    size = fields.Integer(missing=None, validate=lambda n: n >= 1)
    offset = fields.Integer(missing=0, validate=lambda n: n >= 0)


class cmd_FILE_WRITE_Schema(BaseCmdSchema):
    fd = fields.Int(required=True)
    data = fields.String(required=True)
    offset = fields.Integer(missing=0, validate=lambda n: n >= 0)


class cmd_FILE_TRUNCATE_Schema(BaseCmdSchema):
    fd = fields.Int(required=True)
    length = fields.Integer(validate=lambda n: n >= 0)


class cmd_FILE_CLOSE_Schema(BaseCmdSchema):
    fd = fields.Int(required=True)


class FileOperationsAPIMixin(BaseService):

    on_file_changed = event('file_changed')

    next_fd = 0
    file_handles = {}

    @cmd('file_open')
    async def _cmd_FILE_OPEN(self, session, msg):
        msg = cmd_FILE_OPEN_Schema().load(msg)
        fd = await self.file_open(msg['path'])
        return {'status': 'ok', 'fd': fd}

    @cmd('file_read')
    async def _cmd_FILE_READ(self, session, msg):
        msg = cmd_FILE_READ_Schema().load(msg)
        response = await self.file_read(msg['fd'], msg['version'], msg['size'], msg['offset'])
        response['status'] = 'ok'
        return response

    @cmd('file_write')
    async def _cmd_WRITE(self, session, msg):
        msg = cmd_FILE_WRITE_Schema().load(msg)
        await self.file_write(msg['fd'], msg['data'], msg['offset'])
        return {'status': 'ok'}

    @cmd('file_truncate')
    async def _cmd_TRUNCATE(self, session, msg):
        msg = cmd_FILE_TRUNCATE_Schema().load(msg)
        await self.file_truncate(msg['fd'], msg['length'])
        return {'status': 'ok'}

    @cmd('file_close')
    async def _cmd_FILE_CLOSE(self, session, msg):
        msg = cmd_FILE_CLOSE_Schema().load(msg)
        await self.file_close(msg['fd'])
        return {'status': 'ok'}

    async def file_open(self, path, group=None):
        try:
            properties = await self.get_properties(path=path, group=group)
        except FileNotFound:
            try:
                properties = await self.get_properties(path=path, dustbin=True, group=group)
            except FileNotFound:
                raise FileNotFound('Vlob not found.')
        response = await self.file_stat(properties['id'])
        # properties['version'] = response['version']
        # for key, value in self.file_handles.items():
        #     if value == properties:
        #         return key
        fd = self.next_fd
        self.file_handles[fd] = await File.load(self.backend, self.block, properties, response['version'])
        self.next_fd += 1  # TODO overflow?
        return fd

    async def file_read(self, fd, version=None, size=None, offset=0):
        try:
            file = self.file_handles[fd]
        except KeyError:
            raise FileNotFound('File descriptor not found.')
        return await file.read(version, size, offset)

    async def file_write(self, fd, data, offset):
        try:
            file = self.file_handles[fd]
        except KeyError:
            raise FileNotFound('File descriptor not found.')
        await file.write(data, offset)

    async def file_truncate(self, fd, length):
        try:
            file = self.file_handles[fd]
        except KeyError:
            raise FileNotFound('File descriptor not found.')
        await file.truncate(length)

    async def file_close(self, fd):
        try:
            file = self.file_handles[fd]
        except KeyError:
            raise FileNotFound('File descriptor not found.')
        await file.commit()
        # vlob = await self.buffered_vlob.read(properties['id'],
        #                                      properties['read_trust_seed'],
        #                                      properties['version'])
        # blob = vlob['blob']
        # encrypted_blob = decodebytes(blob.encode())
        # blob_key = decodebytes(properties['key'].encode())
        # encryptor = AESCipher()
        # blob = encryptor.decrypt(blob_key, encrypted_blob)
        # blob = json.loads(blob.decode())
        # block_ids = []
        # for block_and_key in blob:
        #     for block in block_and_key['blocks']:
        #         block_ids.append(block['block'])
        # await self.buffered_block.flush(properties['id'], block_ids)
        # await self.buffered_vlob.flush(properties['id'])
        del self.file_handles[fd]
        # properties['version'] += 1
