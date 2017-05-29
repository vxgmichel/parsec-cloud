from base64 import decodebytes, encodebytes
from collections import defaultdict
from datetime import datetime
from uuid import uuid4

from cryptography.hazmat.backends.openssl import backend as openssl
from cryptography.hazmat.primitives import hashes

from parsec.backend.vlob_service import generate_trust_seed
from parsec.core.cache import cache
from parsec.core.synchronizer import synchronizer_factory
from parsec.crypto import AESCipher
from parsec.exceptions import FileNotFound


class BufferedBlock:

    @classmethod
    async def create(cls, service, content):
        self = BufferedBlock()
        self.service = service
        self.id = uuid4().hex
        self.timestamp = int(datetime.utcnow().timestamp())
        await cache.set(self.id, content)
        synchronizer = synchronizer_factory()
        await synchronizer.add_buffered_block(self)
        return self

    async def get_id(self):
        return self.id

    async def read(self):
        content = await cache.get(self.id)
        return {
            'content': content,
            'creation_timestamp': self.timestamp,
            'status': 'ok'  # TODO keep status for all responses?
        }

    async def stat(self):
        return {
            'creation_timestamp': self.timestamp,
            'status': 'ok'  # TODO keep status for all responses?
        }

    async def commit(self):
        content = await cache.get(self.id)
        if content:
            await self.service.create(content, self.id)
        await cache.delete(self.id)
        self.cache_id = uuid4().hex

    async def discard(self):
        await cache.delete(self.id)


class BufferedUserVlob:

    @classmethod
    async def create(cls, service, version=0, blob=''):
        self = BufferedUserVlob()
        self.service = service
        self.version = version
        await cache.set('USER_VLOB', blob)
        synchronizer = synchronizer_factory()
        await synchronizer.add_buffered_user_vlob(self)
        return self

    @classmethod
    async def load(cls,
                   service,
                   version=None):
        user_vlob = await service.user_vlob_read(version)
        self = await BufferedUserVlob.create(service, user_vlob['version'], user_vlob['blob'])
        await cache.set('USER_VLOB', user_vlob['blob'])
        return self

    async def get_id(self):
        return self.id

    async def get_version(self):
        return self.version

    async def set_version(self, version):
        self.version = version

    async def update(self, blob):
        await cache.set('USER_VLOB', blob)

    async def read(self):
        blob = await cache.get('USER_VLOB')
        blob = blob if blob else ''
        return {
            'blob': blob,
            'version': self.version,
            'status': 'ok'  # TODO keep status for all responses?
        }

    async def commit(self):
        blob = await cache.get('USER_VLOB')
        if not blob:
            return
        self.version += 1
        await self.service.user_vlob_update(self.version, blob)
        await cache.delete('USER_VLOB')

    async def discard(self):
        await cache.delete('USER_VLOB')


class BufferedVlob:

    @classmethod
    async def create(cls,
                     service,
                     id=None,
                     version=0,
                     read_trust_seed=generate_trust_seed(),
                     write_trust_seed=generate_trust_seed(),
                     blob=''):  # TODO add blob_key and get decrypted blob method?
        self = BufferedVlob()
        self.service = service
        self.id = id if id else uuid4().hex
        self.version = version
        self.read_trust_seed = read_trust_seed
        self.write_trust_seed = write_trust_seed
        await cache.set(self.id, blob)
        synchronizer = synchronizer_factory()
        await synchronizer.add_buffered_vlob(self)
        return self

    @classmethod
    async def load(cls, service, id, read_trust_seed, write_trust_seed, version=None):
        vlob = await service.vlob_read(id, read_trust_seed, version)
        self = await BufferedVlob.create(service, id, vlob['version'], read_trust_seed, write_trust_seed, vlob['blob'])
        await cache.set(self.id, vlob['blob'])
        return self

    async def get_properties(self):  # TODO rename ? Used by file create
        return {'id': self.id,
                'read_trust_seed': self.read_trust_seed,
                'write_trust_seed': self.write_trust_seed}

    async def get_id(self):
        return self.id

    async def get_version(self):
        return self.version

    async def set_version(self, version):  # TODO used?
        self.version = version

    async def update(self, blob):
        await cache.set(self.id, blob)

    async def read(self):
        blob = await cache.get(self.id)
        blob = blob if blob else ''
        return {
            'id': self.id,
            'blob': blob,
            'version': self.version,
            'status': 'ok'  # TODO keep status for all responses?
        }

    async def commit(self):
        blob = await cache.get(self.id)
        if not blob:
            return
        if not self.version:
            await self.service.vlob_create(blob,
                                           self.id,
                                           self.read_trust_seed,
                                           self.write_trust_seed)
        else:
            self.version += 1
            await self.service.vlob_update(self.id, self.version, self.write_trust_seed, blob)
        await cache.delete(self.id)

    async def discard(self):
        await cache.delete(self.id)
