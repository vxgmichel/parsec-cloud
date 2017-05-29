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


class TestManifestAPI:

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    @pytest.mark.parametrize('path', ['/test', '/test_dir/test'])
    async def test_show_dustbin(self, core_svc, group, path):
        # Empty dustbin
        ret = await core_svc.dispatch_msg({'cmd': 'show_dustbin',
                                                    'group': group})
        assert ret == {'status': 'ok', 'dustbin': []}
        await core_svc.file_create('/foo', group=group)
        await core_svc.file_delete('/foo', group)
        await core_svc.folder_create('/test_dir', parents=False, group=group)
        for i in [1, 2]:
            await core_svc.file_create(path, group=group)
            await core_svc.file_delete(path, group)
            # Global dustbin with one additional file
            ret = await core_svc.dispatch_msg({'cmd': 'show_dustbin',
                                                        'group': group})
            assert ret['status'] == 'ok'
            assert len(ret['dustbin']) == i + 1
            # File in dustbin
            ret = await core_svc.dispatch_msg({'cmd': 'show_dustbin',
                                                        'path': path,
                                                        'group': group})
            assert ret['status'] == 'ok'
            assert len(ret['dustbin']) == i
            # Not found
            ret = await core_svc.dispatch_msg({'cmd': 'show_dustbin',
                                                        'path': '/unknown',
                                                        'group': group})
            assert ret == {'status': 'user_manifest_not_found', 'label': 'Path not found.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'show_dustbin', 'path': '/foo', 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'show_dustbin', 'path': '/foo', 'group': 42},
        {'cmd': 'show_dustbin', 'path': 42},
        {}])
    async def test_bad_msg_show_dustbin(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_history(self, core_svc):
        foo_vlob = await core_svc.file_create('/foo')
        await core_svc.synchronize_manifest()
        bar_vlob = await core_svc.file_create('/bar')
        await core_svc.synchronize_manifest()
        baz_vlob = await core_svc.file_create('/baz')
        await core_svc.synchronize_manifest()
        # Full history
        ret = await core_svc.dispatch_msg({'cmd': 'history'})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 1,
                    'entries': {'added': {}, 'changed': {}, 'removed': {}},
                    'groups': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {}, 'changed': {}, 'removed': {}}

                },
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 3,
                    'entries': {'added': {'/bar': bar_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'removed': {}, 'added': {}, 'changed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {bar_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 4,
                    'entries': {'added': {'/baz': baz_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'removed': {}, 'added': {}, 'changed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {baz_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Partial history starting at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 2})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 3,
                    'entries': {'added': {'/bar': bar_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'removed': {}, 'added': {}, 'changed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {bar_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 4,
                    'entries': {'added': {'/baz': baz_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'removed': {}, 'added': {}, 'changed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {baz_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Partial history ending at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'last_version': 2})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 1,
                    'entries': {'added': {}, 'changed': {}, 'removed': {}},
                    'groups': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'groups': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Summary of full history
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'summary': True})

        assert ret == {
            'status': 'ok',
            'summary_history': {
                'entries': {'added': {'/foo': foo_vlob,
                                      '/bar': bar_vlob,
                                      '/baz': baz_vlob},
                            'changed': {},
                            'removed': {}},
                'groups': {'added': {}, 'changed': {}, 'removed': {}},
                'dustbin': {'added': [], 'removed': []},
                'versions': {'added': {foo_vlob['id']: 1, bar_vlob['id']: 1, baz_vlob['id']: 1},
                             'changed': {},
                             'removed': {}}
            }
        }
        # Summary of partial history
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 2,
                                                    'last_version': 4,
                                                    'summary': True})
        assert ret == {
            'status': 'ok',
            'summary_history': {
                'entries': {'added': {'/bar': bar_vlob,
                                      '/baz': baz_vlob},
                            'changed': {},
                            'removed': {}},
                'groups': {'added': {}, 'changed': {}, 'removed': {}},
                'dustbin': {'added': [], 'removed': []},
                'versions': {'added': {bar_vlob['id']: 1, baz_vlob['id']: 1},
                             'changed': {},
                             'removed': {}}
            }
        }
        # First version > last version
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 4,
                                                    'last_version': 2,
                                                    'summary': True})
        assert ret == {'status': 'bad_versions',
                       'label': 'First version number higher than the second one.'}

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_group_manifest_history(self, core_svc):
        group = 'foo_community'
        foo_vlob = await core_svc.file_create('/foo', group=group)
        bar_vlob = await core_svc.file_create('/bar', group=group)
        baz_vlob = await core_svc.file_create('/baz', group=group)
        # Full history
        ret = await core_svc.dispatch_msg({'cmd': 'history', 'group': group})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 1,
                    'entries': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 3,
                    'entries': {'added': {'/bar': bar_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {bar_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 4,
                    'entries': {'added': {'/baz': baz_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {baz_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Partial history starting at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 2,
                                                    'group': group})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 3,
                    'entries': {'added': {'/bar': bar_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {bar_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 4,
                    'entries': {'added': {'/baz': baz_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'removed': [], 'added': []},
                    'versions': {'added': {baz_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Partial history ending at version 2
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'last_version': 2,
                                                    'group': group})
        assert ret == {
            'status': 'ok',
            'detailed_history': [
                {
                    'version': 1,
                    'entries': {'added': {}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {}, 'changed': {}, 'removed': {}}
                },
                {
                    'version': 2,
                    'entries': {'added': {'/foo': foo_vlob}, 'changed': {}, 'removed': {}},
                    'dustbin': {'added': [], 'removed': []},
                    'versions': {'added': {foo_vlob['id']: 1}, 'changed': {}, 'removed': {}}
                }
            ]
        }
        # Summary of full history
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'summary': True,
                                                    'group': group})

        assert ret == {
            'status': 'ok',
            'summary_history': {
                'entries': {'added': {'/foo': foo_vlob,
                                      '/bar': bar_vlob,
                                      '/baz': baz_vlob},
                            'changed': {},
                            'removed': {}},
                'dustbin': {'added': [], 'removed': []},
                'versions': {'added': {foo_vlob['id']: 1, bar_vlob['id']: 1, baz_vlob['id']: 1},
                             'changed': {},
                             'removed': {}}
            }
        }
        # Summary of partial history
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 2,
                                                    'last_version': 4,
                                                    'summary': True,
                                                    'group': group})
        assert ret == {
            'status': 'ok',
            'summary_history': {
                'entries': {'added': {'/bar': bar_vlob,
                                      '/baz': baz_vlob},
                            'changed': {},
                            'removed': {}},
                'dustbin': {'added': [], 'removed': []},
                'versions': {'added': {bar_vlob['id']: 1, baz_vlob['id']: 1},
                             'changed': {},
                             'removed': {}}
            }
        }
        # First version > last version
        ret = await core_svc.dispatch_msg({'cmd': 'history',
                                                    'first_version': 4,
                                                    'last_version': 2,
                                                    'summary': True,
                                                    'group': group})
        assert ret == {'status': 'bad_versions',
                       'label': 'First version number higher than the second one.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'history', 'first_version': 0},
        {'cmd': 'history', 'last_version': 0},
        {'cmd': 'history', 'summary': 'foo'},
        {'cmd': 'history', 'group': 42},
        {'cmd': 'history', 'bad_field': 'foo'},
        {}])
    async def test_bad_msg_history(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'

    @pytest.mark.xfail
    @pytest.mark.asyncio
    @pytest.mark.parametrize('group', [None, 'foo_community'])
    async def test_restore_manifest(self, core_svc, group):

        def encode_content(content):
            return encodebytes(content.encode()).decode()

        group = 'foo_community'
        dust_vlob = await core_svc.file_create('/dust', encode_content('v1'), group=group)
        await core_svc.synchronize_manifest()
        await core_svc.file_delete('/dust', group=group)
        fd = await core_svc.file_open('/dust', group=group)
        await core_svc.file_create('/foo', encode_content('v1'), group=group)
        await core_svc.synchronize_manifest()
        await core_svc.file_create('/bar', encode_content('v1'), group=group)
        await core_svc.synchronize_manifest()
        await core_svc.file_restore(dust_vlob['id'])
        await core_svc.synchronize_manifest()
        fd = await core_svc.file_open('/dust', group=group)
        await core_svc.file_write(fd, encode_content('v2'), 0)
        await core_svc.synchronize_manifest()
        # Previous version
        ret = await core_svc.dispatch_msg({'cmd': 'restore', 'group': group})
        assert ret == {'status': 'ok'}
        listing = await core_svc.list_dir('/', group)
        assert sorted(listing[1].keys()) == ['bar', 'foo']
        fd = await core_svc.file_open('/dust', group=group)
        file = await core_svc.file_read(fd)
        assert file == {'content': encode_content('v1'), 'version': 3}
        # Restore old version
        ret = await core_svc.dispatch_msg({'cmd': 'restore',
                                                    'version': 4,
                                                    'group': group})
        assert ret == {'status': 'ok'}
        listing = await core_svc.list_dir('/', group)
        assert sorted(listing[1].keys()) == ['foo']
        fd = await core_svc.file_open('/dust', group=group)
        file = await core_svc.file_read(fd)
        assert file == {'content': encode_content('v1'), 'version': 4}
        # Bad version
        ret = await core_svc.dispatch_msg({'cmd': 'restore',
                                                    'version': 10,
                                                    'group': group})
        assert ret == {'status': 'bad_version', 'label': 'Bad version number.'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_msg', [
        {'cmd': 'restore', 'version': 1, 'group': 'share', 'bad_field': 'foo'},
        {'cmd': 'restore', 'version': 1, 'group': 42},
        {'cmd': 'restore', 'version': '42a'},
        {}])
    async def test_bad_msg_restore(self, core_svc, bad_msg):
        ret = await core_svc.dispatch_msg(bad_msg)
        assert ret['status'] == 'bad_msg'
