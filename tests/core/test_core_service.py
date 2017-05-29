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


class TestCoreService:

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_load_user_manifest(self, core_svc, identity_svc):
        await core_svc.file_create('/test')
        await core_svc.path_info('/test')
        ret = await core_svc.dispatch_msg({'cmd': 'user_manifest_load'})
        assert ret == {'status': 'ok'}
        await core_svc.path_info('/test')
        identity = '3C3FA85FB9736362497EB23DC0485AC10E6274C7'
        manifest = await core_svc.get_manifest()
        old_identity = manifest.id
        assert old_identity != identity
        await identity_svc.load_identity(identity)
        ret = await core_svc.dispatch_msg({'cmd': 'user_manifest_load'})
        assert ret == {'status': 'ok'}
        manifest = await core_svc.get_manifest()
        assert manifest.id == identity
        with pytest.raises(UserManifestNotFound):
            await core_svc.path_info('/test')
        await identity_svc.load_identity(old_identity)
        ret = await core_svc.dispatch_msg({'cmd': 'user_manifest_load'})
        assert ret == {'status': 'ok'}
        await core_svc.path_info('/test')

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_get_manifest(self, core_svc):
        manifest = await core_svc.get_manifest()
        assert manifest.id == core_svc.identity.id
        group_manifest = await core_svc.get_manifest('foo_community')
        assert group_manifest.id is not None
        with pytest.raises(UserManifestNotFound):
            await core_svc.get_manifest('unknown')
        with pytest.raises(UserManifestNotFound):
            core_svc.user_manifest = None  # TODO too intrusive
            await core_svc.get_manifest()

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_get_properties(self, core_svc, group):
        foo_vlob = await core_svc.file_create('/foo', group=group)
        bar_vlob = await core_svc.file_create('/bar', group=group)
        await core_svc.file_delete('/bar', group)
        # Lookup group
        group_manifest = await core_svc.get_manifest(group='foo_community')
        group_vlob = await core_svc.get_properties(group='foo_community')
        assert await group_manifest.get_vlob() == group_vlob
        # Lookup foo by path
        vlob = await core_svc.get_properties(path='/foo', dustbin=False, group=group)
        assert vlob == foo_vlob
        with pytest.raises(UserManifestNotFound):
            await core_svc.get_properties(path='/foo', dustbin=True, group=group)
        # Lookup bar by path
        vlob = await core_svc.get_properties(path='/bar', dustbin=True, group=group)
        vlob = deepcopy(vlob)  # TODO use deepcopy?
        del vlob['removed_date']
        del vlob['path']
        assert vlob == bar_vlob
        with pytest.raises(UserManifestNotFound):
            await core_svc.get_properties(path='/bar', dustbin=False, group=group)
        # Lookup foo by id
        vlob = await core_svc.get_properties(id=foo_vlob['id'], dustbin=False, group=group)
        assert vlob == foo_vlob
        with pytest.raises(UserManifestNotFound):
            await core_svc.get_properties(id=foo_vlob['id'], dustbin=True, group=group)
        # Lookup bar by id
        vlob = await core_svc.get_properties(id=bar_vlob['id'], dustbin=True, group=group)
        vlob = deepcopy(vlob)  # TODO use deepcopy?
        del vlob['removed_date']
        del vlob['path']
        assert vlob == bar_vlob
        with pytest.raises(UserManifestNotFound):
            await core_svc.get_properties(id=bar_vlob['id'], dustbin=False, group=group)
