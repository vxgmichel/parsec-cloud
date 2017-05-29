import pytest
import asyncio
import blinker
from io import BytesIO

from parsec.server import WebSocketServer
from parsec.backend import MockedVlobService, InMemoryPubKeyService
from parsec.core.backend_api_service import _patch_service_event_namespace
from parsec.core import BackendAPIService, IdentityService, MockedBackendAPIService
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


async def bootstrap_BackendAPIService(request, event_loop, unused_tcp_port):
    event_loop.set_debug(True)
    # Start a minimal backend server...
    backend_server = WebSocketServer()
    pubkey_svc = InMemoryPubKeyService()
    await pubkey_svc.add_pubkey(JOHN_DOE_IDENTITY, JOHN_DOE_PUBLIC_KEY)
    vlob_svc = MockedVlobService()
    # Patch our server not to share it signals with the core (given they should
    # not share the same interpreter)
    backend_signal_namespace = blinker.Namespace()
    _patch_service_event_namespace(vlob_svc, backend_signal_namespace)
    backend_server.register_service(pubkey_svc)
    backend_server.register_service(vlob_svc)
    server_task = await backend_server.start('localhost', unused_tcp_port, loop=event_loop, block=False)
    # ...then create a BackendAPIService in a core server which will connect to
    backend_api_svc = BackendAPIService('ws://localhost:%s' % unused_tcp_port)
    identity_svc = IdentityService()
    core_server = BaseServer()
    core_server.register_service(backend_api_svc)
    core_server.register_service(identity_svc)
    identity = JOHN_DOE_IDENTITY
    identity_key = BytesIO(JOHN_DOE_PRIVATE_KEY)
    await core_server.bootstrap_services()
    await identity_svc.load(identity, identity_key.read())

    def finalize():
        event_loop.run_until_complete(backend_api_svc.teardown())
        server_task.close()
        event_loop.run_until_complete(server_task.wait_closed())

    request.addfinalizer(finalize)
    return backend_api_svc


async def bootstrap_MockedBackendAPIService(request, event_loop, unused_tcp_port):
    return MockedBackendAPIService()


@pytest.fixture(params=[bootstrap_MockedBackendAPIService, bootstrap_BackendAPIService],
                ids=['mocked', 'backend'])
async def backend_api_svc(request, event_loop, unused_tcp_port):
    return await request.param(request, event_loop, unused_tcp_port)


class TestBackendAPIService:

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_cmd(self, backend_api_svc):
        vlob = await backend_api_svc.vlob_create('foo')
        assert isinstance(vlob, dict)

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_no_backend_leak_event(self, backend_api_svc):
        # Backend and core both run in the same interpreter and on the same
        # event loop for the tests, however in reality they are not supposed
        # to share the same events module.

        vlob = await backend_api_svc.vlob_create('First version')

        def _on_vlob_updated(sender):
            assert False, 'Backend callback should not have been called'

        blinker.signal('on_vlob_updated').connect(_on_vlob_updated)
        await backend_api_svc.vlob_update(vlob['id'], 2, vlob['write_trust_seed'], 'Next version')

    @pytest.mark.xfail
    @pytest.mark.asyncio
    async def test_event(self, backend_api_svc):
        vlob = await backend_api_svc.vlob_create('First version')

        is_callback_called = asyncio.Future()

        def _on_vlob_updated(sender):
            nonlocal is_callback_called
            is_callback_called.set_result(sender)

        await backend_api_svc.connect_event('on_vlob_updated', vlob['id'], _on_vlob_updated)
        await backend_api_svc.vlob_update(vlob['id'], 2, vlob['write_trust_seed'], 'Next version')
        ret = await is_callback_called
        assert ret == vlob['id']
