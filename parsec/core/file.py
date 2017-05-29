from base64 import decodebytes, encodebytes
from collections import defaultdict
import json
import sys
from uuid import uuid4

from cryptography.hazmat.backends.openssl import backend as openssl
from cryptography.hazmat.primitives import hashes

from parsec.backend.vlob_service import generate_trust_seed
from parsec.core.buffers import BufferedBlock, BufferedUserVlob, BufferedVlob
from parsec.core.cache import cache
from parsec.core.synchronizer import synchronizer_factory
from parsec.crypto import AESCipher
from parsec.exceptions import FileNotFound


class File:

    @classmethod
    async def create(cls, backend_service, block_service, content):
        self = File()
        self.backend_service = backend_service
        self.block_service = block_service
        self.buffered_blocks = {}
        self.buffered_vlob = await BufferedVlob.create(backend_service, '')
        blob = [await self._build_file_blocks(content)]
        # Encrypt blob
        blob = json.dumps(blob)
        blob = blob.encode()
        encryptor = AESCipher()
        self.blob_key, encrypted_blob = encryptor.encrypt(blob)
        encrypted_blob = encodebytes(encrypted_blob).decode()
        await self.buffered_vlob.update(encrypted_blob)
        return self

    @classmethod
    async def load(cls, backend_service, block_service, manifest_entry, version):
        self = File()
        self.backend_service = backend_service
        self.block_service = block_service
        self.buffered_blocks = {}
        self.blob_key = decodebytes(manifest_entry['key'].encode())
        del manifest_entry['key']
        self.buffered_vlob = await BufferedVlob.load(backend_service,
                                                     manifest_entry['id'],
                                                     manifest_entry['read_trust_seed'],
                                                     manifest_entry['write_trust_seed'],
                                                     version)
        self.synchronizer = synchronizer_factory()
        return self

    async def get_vlob(self):
        response = await self.buffered_vlob.get_properties()
        response['key'] = encodebytes(self.blob_key).decode()
        return response

    async def read(self, version=None, size=None, offset=0):
        vlob = await self.buffered_vlob.read()
        encrypted_blob = decodebytes(vlob['blob'].encode())
        encryptor = AESCipher()
        blob = encryptor.decrypt(self.blob_key, encrypted_blob)
        blob = json.loads(blob.decode())
        # Get data
        matching_blocks = await self._find_matching_blocks(size, offset)
        data = b''
        data += decodebytes(matching_blocks['pre_included_data'].encode())
        for blocks_and_key in matching_blocks['included_blocks']:
            block_key = blocks_and_key['key']
            block_key = decodebytes(block_key.encode())
            for block_properties in blocks_and_key['blocks']:
                block = await self.block_service.read(block_properties['block'])
                # Decrypt
                block_content = decodebytes(block['content'].encode())
                chunk_data = encryptor.decrypt(block_key, block_content)
                # Check integrity
                digest = hashes.Hash(hashes.SHA512(), backend=openssl)
                digest.update(chunk_data)
                new_digest = digest.finalize()
                assert new_digest == decodebytes(block_properties['digest'].encode())
                data += chunk_data
        data += decodebytes(matching_blocks['post_included_data'].encode())
        data = encodebytes(data).decode()
        return {'content': data, 'version': version}

    async def write(self, data, offset):
        data = decodebytes(data.encode())
        matching_blocks = await self._find_matching_blocks(len(data), offset)
        new_data = decodebytes(matching_blocks['pre_excluded_data'].encode())
        new_data += data
        new_data += decodebytes(matching_blocks['post_excluded_data'].encode())
        new_data = encodebytes(new_data).decode()
        blob = []
        blob += matching_blocks['pre_excluded_blocks']
        blob.append(await self._build_file_blocks(new_data))
        blob += matching_blocks['post_excluded_blocks']
        blob = json.dumps(blob)
        blob = blob.encode()
        encryptor = AESCipher()
        _, encrypted_blob = encryptor.encrypt(blob, self.blob_key)
        encrypted_blob = encodebytes(encrypted_blob).decode()
        await self.buffered_vlob.update(encrypted_blob)

    async def truncate(self, length):
        matching_blocks = await self._find_matching_blocks(length, 0)
        blob = []
        blob += matching_blocks['included_blocks']
        blob.append(await self._build_file_blocks(matching_blocks['post_included_data']))
        blob = json.dumps(blob)
        blob = blob.encode()
        encryptor = AESCipher()
        _, encrypted_blob = encryptor.encrypt(blob, self.blob_key)
        encrypted_blob = encodebytes(encrypted_blob).decode()
        await self.buffered_vlob.update(encrypted_blob)

    # async def stat(self):
    #     # TODO ?
    #     pass

    # async def history(self):
    #     # TODO ?
    #     pass

    # async def restore(self):
    #     # TODO ?
    #     pass

    # async def reencrypt(self):
    #     # TODO ?
    #     pass

    async def commit(self):
        vlob = await self.buffered_vlob.read()
        encrypted_blob = decodebytes(vlob['blob'].encode())
        encryptor = AESCipher()
        blob = encryptor.decrypt(self.blob_key, encrypted_blob)
        blob = json.loads(blob.decode())
        block_ids = []
        for block_and_key in blob:
            for block in block_and_key['blocks']:
                block_ids.append(block['block'])
        for block_id in block_ids:
            try:
                await self.synchronizer.set_synchronizable_flag(self.buffered_blocks[block_id], True)
            except Exception:  # TODO change type
                pass
        await self.synchronizer.set_synchronizable_flag(self.buffered_vlob, True)

    async def discard(self):
        vlob = await self.buffered_vlob.read()
        blob = vlob['blob']
        encrypted_blob = decodebytes(blob.encode())
        encryptor = AESCipher()
        blob = encryptor.decrypt(self.blob_key, encrypted_blob)
        blob = json.loads(blob.decode())
        block_ids = []
        for block_and_key in blob:
            for block in block_and_key['blocks']:
                block_ids.append(block['block'])
        for block_id in block_ids:
            try:
                await self.synchronizer.remove_block(self.buffered_blocks[block_id])
            except Exception:  # TODO change type
                pass
        await self.synchronizer.remove_vlob(self.buffered_vlob)

    async def _block_read(self, id):
        try:
            return self.buffered_blocks[id]
        except KeyError:
            self.block_service.read(id)

    async def _build_file_blocks(self, data):
        if isinstance(data, str):
            data = data.encode()
        data = decodebytes(data)
        # Create chunks
        chunk_size = 4096  # TODO modify size
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        # Force a chunk even if the data is empty
        if not chunks:
            chunks = [b'']
        encryptor = AESCipher()
        block_key, _ = encryptor.encrypt(b'')
        blocks = []
        for chunk in chunks:
            # Digest
            digest = hashes.Hash(hashes.SHA512(), backend=openssl)
            digest.update(chunk)
            chunk_digest = digest.finalize()  # TODO replace with hexdigest ?
            chunk_digest = encodebytes(chunk_digest).decode()
            # Encrypt block
            _, cypher_chunk = encryptor.encrypt(chunk, block_key)
            # Store block
            cypher_chunk = encodebytes(cypher_chunk).decode()
            buffered_block = await BufferedBlock.create(self.block_service, cypher_chunk)
            self.buffered_blocks[buffered_block.id] = buffered_block
            blocks.append({'block': buffered_block.id,
                           'digest': chunk_digest,
                           'size': len(chunk)})
        # New vlob atom
        block_key = encodebytes(block_key).decode()
        blob = {'blocks': blocks,
                'key': block_key}
        return blob

    async def _find_matching_blocks(self, size=None, offset=0):
        if size is None:
            size = sys.maxsize
        pre_excluded_blocks = []
        post_excluded_blocks = []
        vlob = await self.buffered_vlob.read()
        blob = vlob['blob']
        encrypted_blob = decodebytes(blob.encode())
        encryptor = AESCipher()
        blob = encryptor.decrypt(self.blob_key, encrypted_blob)
        blob = json.loads(blob.decode())
        pre_excluded_blocks = []
        included_blocks = []
        post_excluded_blocks = []
        cursor = 0
        pre_excluded_data = b''
        pre_included_data = b''
        post_included_data = b''
        post_excluded_data = b''
        for blocks_and_key in blob:
            block_key = blocks_and_key['key']
            decoded_block_key = decodebytes(block_key.encode())
            for block_properties in blocks_and_key['blocks']:
                cursor += block_properties['size']
                if cursor <= offset:
                    if len(pre_excluded_blocks) and pre_excluded_blocks[-1]['key'] == block_key:
                        pre_excluded_blocks[-1]['blocks'].append(block_properties)
                    else:
                        pre_excluded_blocks.append({'blocks': [block_properties], 'key': block_key})
                elif cursor > offset and cursor - block_properties['size'] < offset:
                    delta = cursor - offset
                    block = await self._block_read(block_properties['block'])
                    content = decodebytes(block['content'].encode())
                    block_data = encryptor.decrypt(decoded_block_key, content)
                    pre_excluded_data = block_data[:-delta]
                    pre_included_data = block_data[-delta:][:size]
                    if size < len(block_data[-delta:]):
                        post_excluded_data = block_data[-delta:][size:]
                elif cursor > offset and cursor <= offset + size:
                    if len(included_blocks) and included_blocks[-1]['key'] == block_key:
                        included_blocks[-1]['blocks'].append(block_properties)
                    else:
                        included_blocks.append({'blocks': [block_properties], 'key': block_key})
                elif cursor > offset + size and cursor - block_properties['size'] < offset + size:
                    delta = offset + size - (cursor - block_properties['size'])
                    block = await self._block_read(block_properties['block'])
                    content = decodebytes(block['content'].encode())
                    block_data = encryptor.decrypt(decoded_block_key, content)
                    post_included_data = block_data[:delta]
                    post_excluded_data = block_data[delta:]
                else:
                    if len(post_excluded_blocks) and post_excluded_blocks[-1]['key'] == block_key:
                        post_excluded_blocks[-1]['blocks'].append(block_properties)
                    else:
                        post_excluded_blocks.append({'blocks': [block_properties],
                                                     'key': block_key})
        pre_included_data = encodebytes(pre_included_data).decode()
        pre_excluded_data = encodebytes(pre_excluded_data).decode()
        post_included_data = encodebytes(post_included_data).decode()
        post_excluded_data = encodebytes(post_excluded_data).decode()
        return {
            'pre_excluded_blocks': pre_excluded_blocks,
            'pre_excluded_data': pre_excluded_data,
            'pre_included_data': pre_included_data,
            'included_blocks': included_blocks,
            'post_included_data': post_included_data,
            'post_excluded_data': post_excluded_data,
            'post_excluded_blocks': post_excluded_blocks
        }
