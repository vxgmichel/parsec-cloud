from io import BytesIO

import pytest

from parsec.core import (CoreService, IdentityService, MetaBlockService,
                         MockedBackendAPIService, MockedBlockService)
from parsec.server import BaseServer
from parsec.exceptions import UserManifestNotFound


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
def core_svc(event_loop, identity_svc):
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


class TestFolderAPI:

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_path_info(self, core_svc, group):
        # Create folders
        await core_svc.folder_create('/countries/France/cities', parents=True, group=group)
        await core_svc.folder_create('/countries/Belgium/cities', parents=True, group=group)
        # Create multiple files
        await core_svc.file_create('/.root', group=group)
        await core_svc.file_create('/countries/index', group=group)
        await core_svc.file_create('/countries/France/info', group=group)
        await core_svc.file_create('/countries/Belgium/info', group=group)
        # Test folders
        ret = await core_svc.dispatch_msg({'cmd': 'path_info',
                                           'path': '/',
                                           'group': group})
        assert ret == {'type': 'folder', 'items': ['.root', 'countries'], 'status': 'ok'}
        ret = await core_svc.dispatch_msg({'cmd': 'path_info',
                                           'path': '/countries',
                                           'group': group})
        assert ret == {'type': 'folder', 'items': ['Belgium', 'France', 'index'], 'status': 'ok'}
        ret = await core_svc.dispatch_msg({'cmd': 'path_info',
                                           'path': '/countries/France/cities',
                                           'group': group})
        assert ret == {'type': 'folder', 'items': [], 'status': 'ok'}
        # Test file
        ret = await core_svc.dispatch_msg({'cmd': 'path_info',
                                           'path': '/countries/France/info',
                                           'group': group})
        assert ret == {'type': 'file', 'status': 'ok'}

        # Test bad list as well
        ret = await core_svc.dispatch_msg({'cmd': 'path_info', 'path': '/dummy', 'group': group})
        assert ret == {'status': 'user_manifest_not_found', 'label': 'Folder or file not found.'}

        ret = await core_svc.dispatch_msg({'cmd': 'path_info',
                                           'path': '/countries/dummy',
                                           'group': group})
        assert ret == {'status': 'user_manifest_not_found', 'label': 'Folder or file not found.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'path_info', 'path': '/foo', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'path_info', 'path': '/foo', 'group': 42},
        {'cmd': 'path_info', 'path': 42},
        {'cmd': 'path_info'}, {}])
    async def test_bad_msg_path_info(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_folder_create(self, core_svc, group):
        # Working
        ret = await core_svc.dispatch_msg({'cmd': 'folder_create',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret['status'] == 'ok'
        # Already exist
        ret = await core_svc.dispatch_msg({'cmd': 'folder_create',
                                           'path': '/test_folder',
                                           'parents': False,
                                           'group': group})
        assert ret == {'status': 'already_exists', 'label': 'Folder already exists.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'folder_create', 'path': '/foo', 'group': 'share', 'parents': True,
         'bad_field': 'foo'},
        {'cmd': 'folder_create', 'path': '/foo', 'group': 'share', 'parents': 'yes'},
        {'cmd': 'folder_create', 'path': '/foo', 'group': 42},
        {'cmd': 'folder_create', 'path': 42},
        {'cmd': 'folder_create'}, {}])
    async def test_bad_msg_folder_create(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_rename_folder(self, core_svc, group):
        await core_svc.folder_create('/test', parents=False, group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'folder_rename',
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
    async def test_rename_folder_and_target_exist(self, core_svc, group):
        await core_svc.folder_create('/test', parents=False, group=group)
        await core_svc.folder_create('/foo', parents=False, group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'folder_rename',
                                           'old_path': '/test',
                                           'new_path': '/foo',
                                           'group': group})
        assert ret == {'status': 'already_exists', 'label': 'File already exists.'}
        await core_svc.path_info('/test', group=group)
        await core_svc.path_info('/foo', group=group)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'folder_rename', 'path': '/foo', 'new_path': '/bar', 'group': 'share',
         'bad_field': 'foo'},
        {'cmd': 'folder_rename', 'path': '/foo', 'new_path': '/bar', 'group': 42},
        {'cmd': 'folder_rename', 'old_path': '/foo', 'new_path': 42},
        {'cmd': 'folder_rename', 'old_path': 42, 'new_path': '/bar'},
        {'cmd': 'folder_rename'}, {}])
    async def test_bad_msg_rename_folder(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_remove_folder(self, core_svc, group):
        # Working
        await core_svc.folder_create('/test_folder', parents=False, group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret == {'status': 'ok'}
        # Not found
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret == {'status': 'user_manifest_not_found', 'label': 'Folder not found.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_cant_remove_root_folder(self, core_svc, group):
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/',
                                           'group': group})
        assert ret == {'status': 'cannot_remove_root', 'label': 'Cannot remove root folder.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_remove_not_empty_folder(self, core_svc, group):
        # Not empty
        await core_svc.folder_create('/test_folder', parents=False, group=group)
        await core_svc.file_create('/test_folder/test', group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret == {'status': 'folder_not_empty', 'label': 'Folder not empty.'}
        # Empty
        ret = await core_svc.dispatch_msg({'cmd': 'file_delete',
                                           'path': '/test_folder/test',
                                           'group': group})
        assert ret == {'status': 'ok'}
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret == {'status': 'ok'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_remove_not_folder(self, core_svc, group):
        await core_svc.file_create('/test_folder', group=group)
        ret = await core_svc.dispatch_msg({'cmd': 'folder_delete',
                                           'path': '/test_folder',
                                           'group': group})
        assert ret == {'status': 'path_is_not_folder', 'label': 'Path is not a folder.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'folder_delete', 'path': '/foo', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'folder_delete', 'path': '/foo', 'group': 42},
        {'cmd': 'folder_delete', 'path': 42},
        {'cmd': 'folder_delete'}, {}])
    async def test_bad_msg_remove_folder(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'
