from base64 import encodebytes
from copy import deepcopy
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
from parsec.exceptions import UserManifestError, UserManifestNotFound
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
    event_loop.run_until_complete(service.group_create('foo_community'))
    yield service
    event_loop.run_until_complete(server.teardown_services())


@pytest.fixture
def identity_svc(event_loop):
    identity = JOHN_DOE_IDENTITY
    identity_key = BytesIO(JOHN_DOE_PRIVATE_KEY)
    service = IdentityService()
    event_loop.run_until_complete(service.load(identity, identity_key.read()))
    return service


@pytest.fixture
def user_vlob_svc(backend_svc):
    return BufferedUserVlob(backend_svc)


@pytest.fixture
def vlob_svc(backend_svc):
    return BufferedVlob(backend_svc)


# @pytest.fixture
# def manifest(event_loop, backend_svc, core_svc, identity_svc, user_vlob_svc, vlob_svc):
#     manifest = Manifest(backend_svc, core_svc, identity_svc, user_vlob_svc, vlob_svc)
#     return manifest

# @pytest.fixture
# def user_manifest_with_group(event_loop, backend_svc, core_svc, identity_svc, user_vlob_svc, vlob_svc):
#     manifest = UserManifest(backend_svc, core_svc, identity_svc, user_vlob_svc, vlob_svc, JOHN_DOE_IDENTITY)
#     event_loop.run_until_complete(manifest.create_group_manifest('foo_community'))
#     return manifest


class TestFileManadementAPI:

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    @pytest.mark.parametrize('path', ['/test', '/test_dir/test'])
    async def test_file_create(self, core_svc, group, path):
        ret = await core_svc.dispatch_msg({'cmd': 'file_create',
                                                    'path': '/test',
                                                    'group': group})
        assert ret['status'] == 'ok'
        assert ret['id'] is not None
        # Already exist
        ret = await core_svc.dispatch_msg({'cmd': 'file_create',
                                                    'path': '/test',
                                                    'group': group})
        assert ret == {'status': 'already_exists', 'label': 'File already exists.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_create', 'group': 'share'},
        {'cmd': 'file_create', 'path': 42},
        {'cmd': 'file_create', 'path': '/foo', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'file_create'}, {}])
    async def test_bad_msg_file_create(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_rename_file(self, core_svc, group):
        await core_svc.file_create('/test', group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'file_rename',
                                                    'old_path': '/test',
                                                    'new_path': '/foo',
                                                    'group': group})
        assert ret['status'] == 'ok'
        with pytest.raises(UserManifestNotFound):
            await core_svc.path_info('/test', group=group)
        await core_svc.path_info('/foo', group=group)

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_rename_file_and_target_exist(self,
                                                core_svc,
                                                group):
        await core_svc.file_create('/test', group=group)
        await core_svc.file_create('/foo', group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'file_rename',
                                                    'old_path': '/test',
                                                    'new_path': '/foo',
                                                    'group': group})
        assert ret == {'status': 'already_exists', 'label': 'File already exists.'}
        await core_svc.path_info('/test', group=group)
        await core_svc.path_info('/foo', group=group)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_rename', 'path': '/foo', 'new_path': '/bar', 'group': 'share',
         'bad_field': 'foo'},
        {'cmd': 'file_rename', 'path': '/foo', 'new_path': '/bar', 'group': 42},
        {'cmd': 'file_rename', 'old_path': '/foo', 'new_path': 42},
        {'cmd': 'file_rename', 'old_path': 42, 'new_path': '/bar'},
        {'cmd': 'file_rename'}, {}])
    async def test_bad_msg_rename_file(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    @pytest.mark.parametrize('path', ['/test', '/test_dir/test'])
    async def test_delete_file(self, core_svc, group, path):
        await core_svc.folder_create('/test_dir', parents=False, group=group)
        for persistent_path in ['/persistent', '/test_dir/persistent']:
            await core_svc.file_create(persistent_path, group=group)
        for i in [1, 2]:
            await core_svc.file_create(path, group=group)
            ret = await core_svc.dispatch_msg({'cmd': 'file_delete',
                                                        'path': path,
                                                        'group': group})
            assert ret == {'status': 'ok'}
            # File not found
            ret = await core_svc.dispatch_msg({'cmd': 'file_delete',
                                                        'path': path,
                                                        'group': group})
            assert ret == {'status': 'user_manifest_not_found', 'label': 'File not found.'}
            # Persistent files
            for persistent_path in ['/persistent', '/test_dir/persistent']:
                await core_svc.path_info(persistent_path, group)

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_delete_not_file(self, core_svc, group):
        await core_svc.folder_create('/test', parents=False, group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'file_delete',
                                                    'path': '/test',
                                                    'group': group})
        assert ret == {'status': 'path_is_not_file', 'label': 'Path is not a file.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_delete', 'path': '/foo', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'file_delete', 'path': '/foo', 'group': 42},
        {'cmd': 'file_delete', 'path': 42},
        {'cmd': 'file_delete'}, {}])
    async def test_bad_msg_delete_file(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    @pytest.mark.parametrize('path', ['/test', '/test_dir/test'])
    async def test_undelete_file(self, core_svc, group, path):
        await core_svc.folder_create('/test_dir', parents=False, group=group)
        file_vlob = await core_svc.file_create(path, group=group)
        await core_svc.file_delete(path, group)
        # Working
        ret = await core_svc.dispatch_msg({'cmd': 'file_undelete',
                                           'id': file_vlob['id'],
                                           'group': group})
        assert ret['status'] == 'ok'
        await core_svc.path_info(path, group)
        # Not found
        ret = await core_svc.dispatch_msg({'cmd': 'file_undelete',
                                           'id': file_vlob['id'],
                                           'group': group})
        assert ret == {'status': 'user_manifest_not_found', 'label': 'Vlob not found.'}
        # Restore path already used
        await core_svc.file_delete(path, group)
        await core_svc.file_create(path, group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'file_undelete',
                                           'id': file_vlob['id'],
                                           'group': group})
        assert ret == {'status': 'already_exists', 'label': 'Restore path already used.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_undelete', 'id': '123', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'file_undelete', 'id': '123', 'group': 42},
        {'cmd': 'file_undelete', 'id': 42},
        {'cmd': 'file_undelete'}, {}])
    async def test_bad_msg_undelete_file(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_stat_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_stat', 'id': '999'})
        assert ret == {'status': 'file_not_found', 'label': 'File not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_stat(self, core_svc):
        # Good file
        with freeze_time('2012-01-01') as frozen_datetime:
            file_vlob = await core_svc.file_create('/test')
            fd = await core_svc.file_open('/test')
            ret = await core_svc.dispatch_msg({'cmd': 'file_stat', 'id': file_vlob['id']})
            ctime = frozen_datetime().timestamp()
            assert ret == {'status': 'ok',
                           'id': file_vlob['id'],
                           'ctime': ctime,
                           'mtime': ctime,
                           'atime': ctime,
                           'size': 0,
                           'version': 1}
            frozen_datetime.tick()
            mtime = frozen_datetime().timestamp()
            content = encodebytes('foo'.encode()).decode()
            await core_svc.file_write(fd, content, 0)
            ret = await core_svc.dispatch_msg({'cmd': 'file_stat', 'id': file_vlob['id']})
            assert ret == {'status': 'ok',
                           'id': file_vlob['id'],
                           'ctime': mtime,
                           'mtime': mtime,
                           'atime': mtime,
                           'size': 3,
                           'version': 1}
            frozen_datetime.tick()
            await core_svc.file_read(fd)  # TODO useless if atime is not modified
            ret = await core_svc.dispatch_msg({'cmd': 'file_stat', 'id': file_vlob['id']})
            assert ret == {'status': 'ok',
                           'id': file_vlob['id'],
                           'ctime': mtime,
                           'mtime': mtime,
                           'atime': mtime,
                           'size': 3,
                           'version': 1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_stat', 'id': 42},
        {'cmd': 'file_stat', 'id': None},
        {'cmd': 'file_stat', 'id': '/test', 'version': 0},
        {'cmd': 'file_stat', 'id': '/test', 'version': 1, 'bad_field': 'foo'},
        {'cmd': 'file_stat'}, {}])
    async def test_bad_msg_stat(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_history_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_history', 'id': '1234'})
        assert ret == {'status': 'file_not_found', 'label': 'File not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_history(self, core_svc):
        with freeze_time('2012-01-01') as frozen_datetime:
            file_vlob = await core_svc.file_create('/test')
            await core_svc.synchronize_manifest()
            fd = await core_svc.file_open('/test')
            id = file_vlob['id']
            original_time = frozen_datetime().timestamp()
            for content in ['this is v2', 'this is v3...']:
                frozen_datetime.tick()
                encoded_content = encodebytes(content.encode()).decode()
                await core_svc.file_write(fd, encoded_content, 0)
                await core_svc.synchronize_manifest()
                fd = await core_svc.file_open('/test')
        # Full history
        ret = await core_svc.dispatch_msg({'cmd': 'file_history', 'id': id})
        assert ret == {
            'status': 'ok',
            'history': [
                {
                    'version': 1,
                    'ctime': original_time,
                    'mtime': original_time,
                    'atime': original_time,
                    'size': 0
                },
                {
                    'version': 2,
                    'ctime': original_time + 1,
                    'mtime': original_time + 1,
                    'atime': original_time + 1,
                    'size': 10
                },
                {
                    'version': 3,
                    'ctime': original_time + 2,
                    'mtime': original_time + 2,
                    'atime': original_time + 2,
                    'size': 13
                }
            ]
        }
        # Partial history starting at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'file_history', 'id': id, 'first_version': 2})
        assert ret == {
            'status': 'ok',
            'history': [
                {
                    'version': 2,
                    'ctime': original_time + 1,
                    'mtime': original_time + 1,
                    'atime': original_time + 1,
                    'size': 10
                },
                {
                    'version': 3,
                    'ctime': original_time + 2,
                    'mtime': original_time + 2,
                    'atime': original_time + 2,
                    'size': 13
                }
            ]
        }
        # Partial history ending at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'file_history', 'id': id, 'last_version': 2})
        assert ret == {
            'status': 'ok',
            'history': [
                {
                    'version': 1,
                    'ctime': original_time,
                    'mtime': original_time,
                    'atime': original_time,
                    'size': 0
                },
                {
                    'version': 2,
                    'ctime': original_time + 1,
                    'mtime': original_time + 1,
                    'atime': original_time + 1,
                    'size': 10
                }
            ]
        }
        # First version = last version
        ret = await core_svc.dispatch_msg({'cmd': 'file_history',
                                           'id': id,
                                           'first_version': 2,
                                           'last_version': 2})
        assert ret == {
            'status': 'ok',
            'history': [
                {
                    'version': 2,
                    'ctime': original_time + 1,
                    'mtime': original_time + 1,
                    'atime': original_time + 1,
                    'size': 10
                }
            ]
        }
        # First version > last version
        ret = await core_svc.dispatch_msg({'cmd': 'file_history',
                                           'id': id,
                                           'first_version': 3,
                                           'last_version': 2})
        assert ret == {'status': 'bad_versions',
                       'label': 'First version number higher than the second one.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_history', 'id': 42},
        {'cmd': 'file_history', 'id': '<id-here>', 'first_version': 0},
        {'cmd': 'file_history', 'id': '<id-here>', 'last_version': 0},
        {'cmd': 'file_history', 'id': '<id-here>', 'first_version': 1, 'last_version': 1,
         'bad_field': 'foo'},
        {'cmd': 'file_history'}, {}])
    async def test_bad_msg_history(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        if bad_msg.get('id') == '<id-here>':
            bad_msg['id'] = file_vlob['id']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_restore_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_restore', 'id': '1234', 'version': 10})
        assert ret == {'status': 'file_not_found', 'label': 'Vlob not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_restore_file(self, core_svc):
        encoded_content = encodebytes('initial'.encode()).decode()
        file_vlob = await core_svc.file_create('/test', encoded_content)
        id = file_vlob['id']
        fd = await core_svc.file_open('/test')
        # Restore file with version 1
        file = await core_svc.file_read(fd)
        assert file == {'content': encoded_content, 'version': 1}
        ret = await core_svc.dispatch_msg({'cmd': 'file_restore', 'id': id})
        await core_svc.synchronize_manifest()
        fd = await core_svc.file_open('/test')
        file = await core_svc.file_read(fd)
        assert file == {'content': encoded_content, 'version': 1}
        # Restore previous version
        for version, content in enumerate(('this is v2', 'this is v3', 'this is v4'), 2):
            encoded_content = encodebytes(content.encode()).decode()
            await core_svc.file_write(fd, encoded_content, 0)
            await core_svc.synchronize_manifest()
            fd = await core_svc.file_open('/test')
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes('this is v4'.encode()).decode()
        assert file == {'content': encoded_content, 'version': 4}
        ret = await core_svc.dispatch_msg({'cmd': 'file_restore', 'id': id})
        await core_svc.synchronize_manifest()
        fd = await core_svc.file_open('/test')
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes('this is v3'.encode()).decode()
        assert file == {'content': encoded_content, 'version': 5}
        # Restore old version
        ret = await core_svc.dispatch_msg({'cmd': 'file_restore', 'id': id, 'version': 2})
        await core_svc.synchronize_manifest()
        fd = await core_svc.file_open('/test')
        file = await core_svc.file_read(fd)
        encoded_content = encodebytes('this is v2'.encode()).decode()
        assert file == {'content': encoded_content, 'version': 6}
        # Bad version
        ret = await core_svc.dispatch_msg({'cmd': 'file_restore', 'id': id, 'version': 10})
        assert ret == {'status': 'bad_version', 'label': 'Bad version number.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_restore', 'id': 42},
        {'cmd': 'file_restore', 'id': '<id-here>', 'version': 0},
        {'cmd': 'file_restore', 'id': '<id-here>', 'version': 1, 'bad_field': 'foo'},
        {'cmd': 'file_restore'}, {}])
    async def test_bad_msg_restore_file(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        if bad_msg.get('id') == '<id-here>':
            bad_msg['id'] = file_vlob['id']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.asyncio
    async def test_reencrypt_not_found(self, core_svc):
        ret = await core_svc.dispatch_msg({'cmd': 'file_reencrypt', 'id': '1234'})
        assert ret == {'status': 'file_not_found', 'label': 'Vlob not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_reencrypt(self, core_svc):
        encoded_content_initial = encodebytes('content 1'.encode()).decode()
        encoded_content_final = encodebytes('content 2'.encode()).decode()
        old_vlob = await core_svc.file_create('/foo', encoded_content_initial)
        ret = await core_svc.dispatch_msg({'cmd': 'file_reencrypt', 'id': old_vlob['id']})
        assert ret['status'] == 'ok'
        del ret['status']
        new_vlob = ret
        for property in old_vlob.keys():
            assert old_vlob[property] != new_vlob[property]
        await core_svc.import_file_vlob('/bar', new_vlob)
        fd = await core_svc.file_open('/bar')
        await core_svc.file_write(fd, encoded_content_final, 0)
        old_fd = await core_svc.file_open('/foo')
        file = await core_svc.file_read(old_fd)
        assert file == {'content': encoded_content_initial, 'version': 1}
        file = await core_svc.file_read(fd)
        assert file == {'content': encoded_content_final, 'version': 1}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'file_reencrypt', 'id': 42},
        {'cmd': 'file_reencrypt', 'id': '<id-here>', 'bad_field': 'foo'},
        {'cmd': 'file_reencrypt'}, {}])
    async def test_bad_msg_reencrypt(self, core_svc, bad_msg):
        file_vlob = await core_svc.file_create('/test')
        if bad_msg.get('id') == '<id-here>':
            bad_msg['id'] = file_vlob['id']
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('length', [0, 4095, 4096, 4097])
    async def test_build_file_blocks(self, core_svc, length):

        def digest(chunk):
            digest = hashes.Hash(hashes.SHA512(), backend=openssl)
            digest.update(chunk)
            chunk_digest = digest.finalize()  # TODO replace with hexdigest ?
            chunk_digest = encodebytes(chunk_digest).decode()
            return chunk_digest

        block_size = 4096
        content = b''.join([str(random.randint(1, 9)).encode() for i in range(0, length)])
        encoded_content = encodebytes(content).decode()
        blocks = await core_svc._build_file_blocks(encoded_content, '123')
        assert sorted(blocks.keys()) == ['blocks', 'key']
        assert isinstance(blocks['blocks'], list)
        required_blocks = int(len(content) / block_size)
        if not len(content) or len(content) % block_size:
            required_blocks += 1
        assert len(blocks['blocks']) == required_blocks
        for index, block in enumerate(blocks['blocks']):
            assert sorted(block.keys()) == ['block', 'digest', 'size']
            assert block['block']
            length = len(content) - index * block_size
            length = block_size if length > block_size else length
            assert block['size'] == length
            assert block['digest'] == digest(content[index * block_size:index + 1 * block_size])
