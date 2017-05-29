from base64 import encodebytes, decodebytes
from copy import deepcopy
import json
from io import BytesIO
import random

from cryptography.hazmat.backends.openssl import backend as openssl
from cryptography.hazmat.primitives import hashes
from freezegun import freeze_time
import pytest

from parsec.core.buffers import BufferedBlock, BufferedUserVlob, BufferedVlob
from parsec.core import (CoreService, IdentityService, MetaBlockService,
                         MockedBackendAPIService, MockedBlockService)
from parsec.core.manifest import GroupManifest, Manifest, UserManifest
from parsec.crypto import AESCipher
from parsec.exceptions import UserManifestError, UserManifestNotFound, FileNotFound, VlobNotFound
from parsec.server import BaseServer


JOHN_DOE_IDENTITY = 'John_Doe'
JOHN_DOE_PRIVATE_KEY = b"""
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxWDIKNqyESM6G
Eqc84DT8OI5114c5lBXTmqTCoMstMZF0uXBawMqjg4QQ7SaTeVBgzGiGjRW8jAWm
7CDSFAGVYkZbno0aga5saaYusGF7oeFtOHp4iD/DNccURoXuN5uAKi+M5+kMHP9h
ipV2zI9P5cvnpu0Ixw+D9trv+0hp9G97Uy881NLO2C6iveAfRO7ULZ0pDzsE+DLT
Y0kbfp44nYvZD3iLy9k9YThNz09JOpzPmQ8MZz4HW+gal+7FYS4nis8dhx8CFz2U
wLRMET13IGkTzf9PJG2u/P4l5e8xDiS7WB/vB7YZeZn1rVOVOfYyCKSAwHHdeArn
J8IgtzbTAgMBAAECggEAXYD80TnGd/DTQwlut8AW76z6H9PFbmxPncP5fsy8k1WB
NaPYQ2FG9jOPXEVNg5AA+yiLK/YTMdg52qrBG0KFGzg3lHLiPsmFJ5AEmLVSkJbn
fmi62fYseEZQcrZEQzd6e3bCn25fB436cHlbGMn9/chRXBA9BdW+rntnMASzR3lC
xYJ4os6BfUHzYvihAJnQfw5N5rXOuGIEZdmnFq3KyogvuHdns1JakDr9ibkUC7Tb
QWnhyN4563B8Jp6CgznKQ+lgpVOAk4AUPX/rIr16nJuJm2JP+qmrg+1pox4Khuit
lO6U6bnKe8mAlPHRiN0yxuXcyyFAE2nuU1XKP3YcAQKBgQDcZkJXSfV1JFfsUDs5
12t+wK3CiV+mixKRBmVS0/yYAmd/o3riPrOGYlK/iDPnOOioU7ssVJf0bVQ353EH
MuOjMx9g5bBWtDREnCRU+R8UYPCmfytmGE7dddh4luLVHTacm9XCNPnw/Sm5jZ+j
YZKjwESxrUn5an68idbPYfMbAQKBgQDN/ZIu9jZ5oNCI72WQLcePVZvSd3k/tFib
8ujLvHR8L3ZDrkZGpv3gHs4P8sunVAObvZLMCraByqwqEIxo/T9X2g/qCrRCNtsE
fMQUCDAK7sGiuzDKdcBfiUh1BL0Xo/JoJmm2DQpvO227G5fAzpf1hhren2EcFmFE
Txc1PID10wKBgEYEZob8g/IW/aehRW92tDusUoc+xRhPjjJsabwKhHB2MxMliGBf
swC6M7eNOY/3UFJJZ2kJ5sxL/zlTWWEEFbU/BHTwAzlIPmKdiB1Gl00ODuWV+N+S
UVuhmIeWx7EUesj96MattcmNY7gC+fgZg1BqQGiBuMJ3xpN25rszTtwBAoGAXGxi
k7mbFZWHG3m2aytvN6ukn5lFiMTFYStrMkabSUEOYi2mkHrKvC12LYe1wp0ahV1Y
qT5BRxkFiFYmedDvA97udwdYe8EbIfdNDuPhknYv4XD14lFVAEibfw2iPiIsWHir
w6g0P1Y91M77luHbIqmKEssWCkEsYTbPZe6AuksCgYEAru15dXKn7wms3FkGXVDW
uQa9dbPvHEcZg+sxXISSscACHN1JiGcJNviSIBd4nubdkH6d/4qhLnZLcVobgLM3
HsozFxThyyrIrPg0M6c4fNJGFgHZUiIv4DR1clqszeuA0oT1ODDxBVhnTB1gHbep
XQ7BVDVuUOTB2k6loHR3LE8=
-----END PRIVATE KEY-----
"""
JOHN_DOE_PUBLIC_KEY = b"""
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsVgyCjashEjOhhKnPOA0
/DiOddeHOZQV05qkwqDLLTGRdLlwWsDKo4OEEO0mk3lQYMxoho0VvIwFpuwg0hQB
lWJGW56NGoGubGmmLrBhe6HhbTh6eIg/wzXHFEaF7jebgCovjOfpDBz/YYqVdsyP
T+XL56btCMcPg/ba7/tIafRve1MvPNTSztguor3gH0Tu1C2dKQ87BPgy02NJG36e
OJ2L2Q94i8vZPWE4Tc9PSTqcz5kPDGc+B1voGpfuxWEuJ4rPHYcfAhc9lMC0TBE9
dyBpE83/TyRtrvz+JeXvMQ4ku1gf7we2GXmZ9a1TlTn2MgikgMBx3XgK5yfCILc2
0wIDAQAB
-----END PUBLIC KEY-----
"""


@pytest.fixture
def backend_svc():
    return MockedBackendAPIService()


@pytest.fixture
def core_svc(event_loop, backend_svc, identity_svc):
    service = CoreService()
    block_service = MetaBlockService(backends=[MockedBlockService, MockedBlockService])
    server = BaseServer()
    server.register_service(service)
    server.register_service(identity_svc)
    server.register_service(block_service)
    server.register_service(MockedBackendAPIService())
    event_loop.run_until_complete(server.bootstrap_services())
    event_loop.run_until_complete(service.load_user_manifest())
    yield service
    event_loop.run_until_complete(server.teardown_services())


@pytest.fixture
def identity_svc(event_loop):
    identity = JOHN_DOE_IDENTITY
    identity_key = BytesIO(JOHN_DOE_PRIVATE_KEY)
    service = IdentityService()
    event_loop.run_until_complete(service.load(identity, identity_key.read()))
    return service


class TestFileOperationsAPI:

    @pytest.mark.asyncio
    async def test_open_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_open', 'path': '/unknown'})
        assert ret == {'status': 'file_not_found', 'label': 'Vlob not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_open(self, core_svc):
        await core_svc.file_create('/test')
        ret = await core_svc.dispatch_msg({'cmd': 'file_open', 'path': '/test'})
        assert sorted(list(ret.keys())) == ['fd', 'status']
        assert ret['status'] == 'ok'
        ret_2 = await core_svc.dispatch_msg({'cmd': 'file_open', 'path': '/test'})
        assert ret == ret_2

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_open', 'path': '/test', 'bad_field': 'foo'},
        {}])
    async def test_bad_msg_open(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_read_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_read',
                                           'fd': '999'})
        assert ret == {'status': 'file_not_found', 'label': 'File descriptor not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_read(self, core_svc):
        await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        # Empty file
        ret = await core_svc.dispatch_msg({'cmd': 'file_read', 'fd': fd})
        assert ret == {'status': 'ok', 'content': '', 'version': 1}
        # Not empty file
        content = 'This is a test content.'
        encoded_content = encodebytes(content.encode()).decode()
        await core_svc.file_write(fd, encoded_content, 0)
        ret = await core_svc.dispatch_msg({'cmd': 'file_read', 'fd': fd})
        assert ret == {'status': 'ok', 'content': encoded_content, 'version': 1}
        # Offset
        offset = 5
        encoded_content = encodebytes(content[offset:].encode()).decode()
        ret = await core_svc.dispatch_msg({'cmd': 'file_read', 'fd': fd, 'offset': offset})
        assert ret == {'status': 'ok', 'content': encoded_content, 'version': 1}
        # Size
        size = 9
        encoded_content = encodebytes(content[offset:][:size].encode()).decode()
        ret = await core_svc.dispatch_msg({'cmd': 'file_read',
                                           'fd': fd,
                                           'size': size,
                                           'offset': offset})
        assert ret == {'status': 'ok', 'content': encoded_content, 'version': 1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_read', 'fd': 'abc'},
        {'cmd': 'file_read', 'fd': None},
        {'cmd': 'file_read', 'fd': '<fd-here>', 'size': 0},
        {'cmd': 'file_read', 'fd': '<fd-here>', 'offset': -1},
        {'cmd': 'file_read', 'fd': '<fd-here>', 'size': 1, 'offset': -1},
        {'cmd': 'file_read', 'fd': '<fd-here>', 'size': 1, 'offset': 0, 'bad_field': 'foo'},
        {'cmd': 'file_read'}, {}])
    async def test_bad_msg_read(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        file_vlob = await core_svc.file_stat(file_vlob['id'])
        if bad_msg.get('fd') == '<fd-here>':
            bad_msg['fd'] = fd
        if bad_msg.get('version') == '<version-here>':
            bad_msg['version'] = file_vlob['version']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_write_not_found(self, core_svc):
        data = encodebytes('foo'.encode()).decode()
        ret = await core_svc.dispatch_msg({'cmd': 'file_write',
                                           'fd': '1234',
                                           'data': data})
        assert ret == {'status': 'file_not_found', 'label': 'File descriptor not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_write(self, core_svc):
        await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        # Check with empty and not empty file
        for content in ['this is v2 content', 'this is v3 content']:
            encoded_data = encodebytes(content.encode()).decode()
            ret = await core_svc.dispatch_msg({'cmd': 'file_write',
                                               'fd': fd,
                                               'data': encoded_data})
            assert ret == {'status': 'ok'}
            file = await core_svc.file_read(fd)
            assert file == {'content': encoded_data, 'version': 1}
        # Offset
        encoded_data = encodebytes('v4'.encode()).decode()
        ret = await core_svc.dispatch_msg({'cmd': 'file_write',
                                           'fd': fd,
                                           'data': encoded_data,
                                           'offset': 8})
        assert ret == {'status': 'ok'}
        file = await core_svc.file_read(fd)
        encoded_data = encodebytes('this is v4 content'.encode()).decode()
        assert file == {'content': encoded_data, 'version': 1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_write', 'fd': 'abc'},
        {'cmd': 'file_write', 'fd': None},
        {'cmd': 'file_write', 'fd': '<fd-here>', 'data': 'YQ==\n', 'offset': -1},
        {'cmd': 'file_write', 'fd': '<fd-here>', 'data': 'YQ==\n', 'offset': 0, 'bad_field': 'foo'},
        {'cmd': 'file_write'}, {}])
    async def test_bad_msg_write(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        file_vlob = await core_svc.file_stat(file_vlob['id'])
        if bad_msg.get('fd') == '<fd-here>':
            bad_msg['fd'] = fd
        if bad_msg.get('version') == '<version-here>':
            bad_msg['version'] = file_vlob['version']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_truncate_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_truncate',
                                           'fd': '1234',
                                           'length': 7})
        assert ret == {'status': 'file_not_found', 'label': 'File descriptor not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_truncate(self, core_svc):
        file_vlob = await core_svc.file_create('/foo')
        fd = await core_svc.file_open('/foo')
        # Encoded contents
        block_size = 4096
        content = b''.join([str(random.randint(1, 9)).encode() for i in range(0, block_size + 1)])
        encoded_content = encodebytes(content).decode()
        # Blocks
        blocks = await core_svc._build_file_blocks(encoded_content, file_vlob['id'])
        # Write content
        blob = json.dumps([blocks])
        blob = blob.encode()
        blob_key = decodebytes(file_vlob['key'].encode())
        encryptor = AESCipher()
        _, encrypted_blob = encryptor.encrypt(blob, blob_key)
        encrypted_blob = encodebytes(encrypted_blob).decode()
        id = file_vlob['id']
        await core_svc.buffered_vlob.update(
            vlob_id=id,
            version=1,
            blob=encrypted_blob,
            trust_seed=file_vlob['write_trust_seed'])
        # Truncate full length
        ret = await core_svc.dispatch_msg({'cmd': 'file_truncate',
                                           'fd': fd,
                                           'length': block_size + 1})
        assert ret == {'status': 'ok'}
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes(content[:block_size + 1]).decode()
        assert file == {'content': encoded_content, 'version': 1}
        # Truncate block length
        ret = await core_svc.dispatch_msg({'cmd': 'file_truncate',
                                           'fd': fd,
                                           'length': block_size})
        assert ret == {'status': 'ok'}
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes(content[:block_size]).decode()
        assert file == {'content': encoded_content, 'version': 1}
        # Truncate shorter than block length
        ret = await core_svc.dispatch_msg({'cmd': 'file_truncate',
                                           'fd': fd,
                                           'length': block_size - 1})
        assert ret == {'status': 'ok'}
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes(content[:block_size - 1]).decode()
        assert file == {'content': encoded_content, 'version': 1}
        # Truncate empty
        ret = await core_svc.dispatch_msg({'cmd': 'file_truncate',
                                           'fd': fd,
                                           'length': 0})
        assert ret == {'status': 'ok'}
        file = await core_svc.file_read(fd)
        assert file == {'content': '', 'version': 1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_truncate', 'fd': 'abc'},
        {'cmd': 'file_truncate', 'fd': None},
        {'cmd': 'file_truncate', 'fd': '<fd-here>', 'length': -1},
        {'cmd': 'file_truncate', 'fd': '<fd-here>', 'length': 0, 'bad_field': 'foo'},
        {'cmd': 'file_truncate'}, {}])
    async def test_bad_msg_truncate(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        file_vlob = await core_svc.file_stat(file_vlob['id'])
        if bad_msg.get('fd') == '<fd-here>':
            bad_msg['fd'] = fd
        if bad_msg.get('version') == '<version-here>':
            bad_msg['version'] = file_vlob['version']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_close_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_close', 'fd': 999})
        assert ret == {'status': 'file_not_found', 'label': 'File descriptor not found.'}

    @pytest.mark.asyncio
    async def test_close(self, core_svc):
        await core_svc.file_create('/test')
        fd = await core_svc.file_open('/test')
        ret = await core_svc.dispatch_msg({'cmd': 'file_close', 'fd': fd})
        assert ret == {'status': 'ok'}
        ret = await core_svc.dispatch_msg({'cmd': 'file_close', 'fd': fd})
        assert ret == {'status': 'file_not_found', 'label': 'File descriptor not found.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_close', 'fd': '/test', 'bad_field': 'foo'},
        {}])
    async def test_bad_msg_open(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_find_matching_blocks_not_found(self, core_svc):
        with pytest.raises(FileNotFound):
            await core_svc._find_matching_blocks('1234')

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_find_matching_blocks(self, core_svc):
        block_size = 4096
        # Contents
        contents = {}
        total_length = 0
        for index, length in enumerate([block_size + 1,
                                        block_size - 1,
                                        block_size,
                                        2 * block_size + 2,
                                        2 * block_size - 2,
                                        2 * block_size]):
            content = b''.join([str(random.randint(1, 9)).encode() for i in range(0, length)])
            contents[index] = content
            total_length += length
        # Encoded contents
        encoded_contents = {}
        for index, content in contents.items():
            encoded_contents[index] = encodebytes(contents[index]).decode()
        # Blocks
        blocks = {}
        for index, encoded_content in encoded_contents.items():
            blocks[index] = await core_svc._build_file_blocks(encoded_content, '123')
        # Create file
        blob = json.dumps([blocks[i] for i in range(0, len(blocks))])
        blob = blob.encode()
        file_vlob = await core_svc.file_create('/foo')
        blob_key = decodebytes(file_vlob['key'].encode())
        encryptor = AESCipher()
        _, encrypted_blob = encryptor.encrypt(blob, blob_key)
        encrypted_blob = encodebytes(encrypted_blob).decode()
        await core_svc.buffered_vlob.update(
            vlob_id=file_vlob['id'],
            version=2,
            blob=encrypted_blob,
            trust_seed=file_vlob['write_trust_seed'])
        # All matching blocks
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'])
        assert matching_blocks == {'pre_excluded_blocks': [],
                                   'pre_excluded_data': '',
                                   'pre_included_data': '',
                                   'included_blocks': [blocks[i] for i in range(0, len(blocks))],
                                   'post_included_data': '',
                                   'post_excluded_data': '',
                                   'post_excluded_blocks': []
                                   }
        # With offset
        delta = 10
        offset = (blocks[0]['blocks'][0]['size'] + blocks[0]['blocks'][1]['size'] +
                  blocks[1]['blocks'][0]['size'] + blocks[2]['blocks'][0]['size'] - delta)
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'],
                                                               version=2,
                                                               offset=offset)
        pre_excluded_data = contents[2][:blocks[2]['blocks'][0]['size'] - delta]
        pre_included_data = contents[2][-delta:]
        encoded_pre_excluded_data = encodebytes(pre_excluded_data).decode()
        encoded_pre_included_data = encodebytes(pre_included_data).decode()
        assert matching_blocks == {'pre_excluded_blocks': [blocks[0], blocks[1]],
                                   'pre_excluded_data': encoded_pre_excluded_data,
                                   'pre_included_data': encoded_pre_included_data,
                                   'included_blocks': [blocks[i] for i in range(3, 6)],
                                   'post_included_data': '',
                                   'post_excluded_data': '',
                                   'post_excluded_blocks': []
                                   }
        # With small size
        delta = 10
        size = 5
        offset = (blocks[0]['blocks'][0]['size'] + blocks[0]['blocks'][1]['size'] +
                  blocks[1]['blocks'][0]['size'] + blocks[2]['blocks'][0]['size'] - delta)
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'],
                                                               version=2,
                                                               offset=offset,
                                                               size=size)
        pre_excluded_data = contents[2][:blocks[2]['blocks'][0]['size'] - delta]
        pre_included_data = contents[2][-delta:][:size]
        post_excluded_data = contents[2][-delta:][size:]
        encoded_pre_excluded_data = encodebytes(pre_excluded_data).decode()
        encoded_pre_included_data = encodebytes(pre_included_data).decode()
        encoded_post_excluded_data = encodebytes(post_excluded_data).decode()
        assert matching_blocks == {'pre_excluded_blocks': [blocks[0], blocks[1]],
                                   'pre_excluded_data': encoded_pre_excluded_data,
                                   'pre_included_data': encoded_pre_included_data,
                                   'included_blocks': [],
                                   'post_included_data': '',
                                   'post_excluded_data': encoded_post_excluded_data,
                                   'post_excluded_blocks': [blocks[i] for i in range(3, 6)]
                                   }
        # With big size
        delta = 10
        size = delta
        size += blocks[3]['blocks'][0]['size']
        size += blocks[3]['blocks'][1]['size']
        size += blocks[3]['blocks'][2]['size']
        size += 2 * delta
        offset = (blocks[0]['blocks'][0]['size'] + blocks[0]['blocks'][1]['size'] +
                  blocks[1]['blocks'][0]['size'] + blocks[2]['blocks'][0]['size'] - delta)
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'],
                                                               version=2,
                                                               offset=offset,
                                                               size=size)
        pre_excluded_data = contents[2][:-delta]
        pre_included_data = contents[2][-delta:]
        post_included_data = contents[4][:2 * delta]
        post_excluded_data = contents[4][:block_size][2 * delta:]
        encoded_pre_excluded_data = encodebytes(pre_excluded_data).decode()
        encoded_pre_included_data = encodebytes(pre_included_data).decode()
        encoded_post_included_data = encodebytes(post_included_data).decode()
        encoded_post_excluded_data = encodebytes(post_excluded_data).decode()
        partial_block_4 = deepcopy(blocks[4])
        del partial_block_4['blocks'][0]
        assert matching_blocks == {'pre_excluded_blocks': [blocks[0], blocks[1]],
                                   'pre_excluded_data': encoded_pre_excluded_data,
                                   'pre_included_data': encoded_pre_included_data,
                                   'included_blocks': [blocks[3]],
                                   'post_included_data': encoded_post_included_data,
                                   'post_excluded_data': encoded_post_excluded_data,
                                   'post_excluded_blocks': [partial_block_4, blocks[5]]
                                   }
        # With big size and no delta
        size = blocks[3]['blocks'][0]['size']
        size += blocks[3]['blocks'][1]['size']
        size += blocks[3]['blocks'][2]['size']
        offset = (blocks[0]['blocks'][0]['size'] + blocks[0]['blocks'][1]['size'] +
                  blocks[1]['blocks'][0]['size'] + blocks[2]['blocks'][0]['size'])
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'],
                                                               version=2,
                                                               offset=offset,
                                                               size=size)
        assert matching_blocks == {'pre_excluded_blocks': [blocks[0], blocks[1], blocks[2]],
                                   'pre_excluded_data': '',
                                   'pre_included_data': '',
                                   'included_blocks': [blocks[3]],
                                   'post_included_data': '',
                                   'post_excluded_data': '',
                                   'post_excluded_blocks': [blocks[4], blocks[5]]
                                   }
        # # With total size
        matching_blocks = await core_svc._find_matching_blocks(file_vlob['id'],
                                                               version=2,
                                                               offset=0,
                                                               size=total_length)
        assert matching_blocks == {'pre_excluded_blocks': [],
                                   'pre_excluded_data': '',
                                   'pre_included_data': '',
                                   'included_blocks': [blocks[i] for i in range(0, 6)],
                                   'post_included_data': '',
                                   'post_excluded_data': '',
                                   'post_excluded_blocks': []
                                   }
