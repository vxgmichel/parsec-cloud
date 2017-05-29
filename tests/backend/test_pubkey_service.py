import pytest
import json
from unittest.mock import patch
import asyncio

from parsec.server import BaseServer, BaseClientContext
from parsec.backend import InMemoryPubKeyService
from parsec.crypto import RSACipher
from parsec.session import AuthSession
from parsec.exceptions import PubKeyError, PubKeyNotFound

from tests.common import can_side_effect_or_skip
from tests.backend.common import init_or_skiptest_parsec_postgresql


ALICE_PRIVATE_RSA = b"""
-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC1sPyayftfemFy
919RTseOgUbMjV04Cmls/Nd2t5hTM8TFVrsIelbcvdyQPQGxWj0/bskQ3qgM9+BT
ILep7iExrUfad0fTUlhcTr0+P6R47kLc1jjWMFp+EF94/b9vOAbcBFhQ4um+Kzr+
u8fGEV/EHgsStv8SlWD/8q3w3yBHHwIUY0WhiLtAepVhaFl4O+RnQii5PUGKbNmL
qznSxgSS7qkQwJs3uQs7t+9e32E0QA8H1BJRJGS6ZRtL/XX8ifXW3JF/YW52K7u2
CaeMhI2AboOo34jTHdsSOwkY36Se9gRRqqmBHpkHSBFrgd+JWnpJeHHdHbSEcvAz
y0J2B5YDAgMBAAECggEBAI5ij+NCDJOrVXkCMRmH8k1UNEaTNg1341y83KG1iVMS
y86WhGZFcZL120bR+GSjQkJWsI1e4DWhf5PFnQk3i53hnOCw/4G1E3frYNTKjTsU
BSQJYWtBY3lNnXQ07vPa5U1AvnFNEj6spEQCprEq5nDL1oOpE0Xd+LE73mJYiXQR
nlU2/4jLeJZqULNV7JYwmv0FqxIYTkSuB0waU6ZO0HryXl8RyVaNCh4ezpZ8/iO9
wsiOjzhsaExjIMAl0wwLN7rkDg/glozOAg1Kqjz7oZTEkoaaRsY+HW3WbtsjGrdl
80BtivSt6SH7RAPKxaCg8zvgN+1CTh2gjq5AE2YEfAECgYEA7J1zR8YZbFwyvDty
s3Pv3MBPSTIdIRU2TGDApchA9k4lOh/h4G8YHIvSiq2/KholFIFioL0fZqbrdynq
g+IJRdSOfVrvjDMtQrKWAktI1yDHseNVMtmI6tY7lkr/HFVBE3YOzhcJIiHKtRhp
/PRr/9X6+jUooC111JAL8feOggECgYEAxJOd40prjEWMkHNmpJsIbc6IUtX/qSnU
qRmparii/7H1ZS8bIUihFqrJjR6wDOjJ/hPaLzv8GLRUGNgiwCtUQGzLnTO6qSMa
8zKSYy1BhYNsvklJh7uZjkOLi3/qFVy/1CuIGf0sOD8dR28lsfIfz5FTP9827t3T
nDx6ry88EAMCgYEAjycLMedWRkrZnyxQTuXbvrDSxzP8j6Fnwnne0+3974aD21Ci
tchAzDSD4scPmwdKW8eTxi/lqZNfbi775WKBva/FrW2w9B+aSHoHa8fkf9MjPiqN
xz/5KCsIzfr9sUSbJI0Ok/0312CeZUYqCZJPLO9m8q3qsE4QtdOYhoIBxAECgYAk
eHq/k6wWb0Tf+/kcKwNKVBoovDldqjwhT3iGK1FafSrZJf8zkqUIbpFMVFg3XO00
7Xv6bXedIb/EiD0SSDeuaEDynolQHgo++q/8JIZWfgar2y8ANscLhO6b6BSG+BlD
BPyQTc9pdBQ7j5x/wjsyqHS4kfJAKPm8r9/XJr1O/wKBgQDPU7q/RA2/9a+uJiI6
lLnOLzgTVzRgX235jeye5LBCNwb1wOWw7Uw80HwMDh+Dw93tecffmGf4C9XE1COU
TDB4jqWe6uxWwHTyPdNdusERRASdlvWqg24bwfcFl4pb0StuRhhNtrvRI77FLAr8
vrkxRnyFlPoVN3/ufyxjZ7pfng==
-----END PRIVATE KEY-----
"""
ALICE_PUBLIC_RSA = b"""
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtbD8msn7X3phcvdfUU7H
joFGzI1dOAppbPzXdreYUzPExVa7CHpW3L3ckD0BsVo9P27JEN6oDPfgUyC3qe4h
Ma1H2ndH01JYXE69Pj+keO5C3NY41jBafhBfeP2/bzgG3ARYUOLpvis6/rvHxhFf
xB4LErb/EpVg//Kt8N8gRx8CFGNFoYi7QHqVYWhZeDvkZ0IouT1BimzZi6s50sYE
ku6pEMCbN7kLO7fvXt9hNEAPB9QSUSRkumUbS/11/In11tyRf2Fudiu7tgmnjISN
gG6DqN+I0x3bEjsJGN+knvYEUaqpgR6ZB0gRa4HfiVp6SXhx3R20hHLwM8tCdgeW
AwIDAQAB
-----END PUBLIC KEY-----
"""

BOB_PRIVATE_RSA = b"""
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCyLb1hoTabwhTq
6LTk2Hft2e98WzT0Hl8/9DdHb3g1bVfu933aiMohIDSoUXNMhPN1B2x7fwv8aikj
VBwCqCN3ObuLFtRYAj7s0r8LJmBot3gC4z/D1SfLzg2FbJklLW0bDl02LJk0Hqro
1Y8HNrbhizaYdzVOHaQQFoHtDOIprWZbEPHQeoSSATkfSXvUmuJoYK0gRRV2dXoD
N+ZbLRY8qhUohdfMsMDZrC6RL/fp8N6tLpBYNdzxHVvZVa0JuqyHiff8kvSYQHvf
xY2K25/c881uREUBiH3tLeTS6X25Semhigd1b62AupYjjrKkHWg9/HSgJgiJY+hK
CIWDnJmXAgMBAAECggEAdaVNzhCsBdv4A8Ly7ccgKKQlRG7UX/tN2ORfO4OWU3CH
BvnS12BKVeT1380n2/ZM6ZClSEVynI9b6j+23uo1wJsWAZhpTFLvSV89VRdZwMqj
KwTxLVSomiDLPLWfyLRdveeWFBcOcSNupZ2cep7d0b4hpjnPsGxRz22NC//dtLX3
JAUQ0jxwl72TsgttnMCXaffXNqb7fZeteTFryPElFlZ6xq/Jh9lG2Ad8HggwISHT
cujEW9kuKc3awj1Q5W9BN2gXf9rpBJ/yNwZjqXdY/noq4hWZ1QISRQtTQWL6D91O
aEiE1Iu6FScpQpTAawksxAD8s62Ym3VMkni27uYewQKBgQDbY8GX5tnS7oFjnSAd
/TkprXbG0zh/C680sa+ZyRdLR0wetDeQHqZNo25ExqFEXMeLIgxVyuoMbWsfNEmM
jud7iK84yq7CzBxbaF9Zl3Dqd8cZWZqdkJwVjCK6PZmfn+hh6x0Ki2WeBY32Is2s
tVw4ykEt8bwnJqup45nL+1Cd8wKBgQDP6XcUG277cknb8oZSYwLx+4WCQePrcHuF
WU0Dq7VBF/FuiyZ5tVAT+qpFYvol46qZzF/zDv2pyDREyN/nSmgwh7iI6M1IR/6B
7ANddXUzcaaQ/DDlVNWloJ68I07acCNNp55GJhiNFfqDdBr6Gg0Bv9lfdC+X+Qwo
NFPhkxrqzQKBgARdf8Sd+0ePJ1PsFG+EUlbZ9LsQCNe+S8Yoou3Uano8+O7DdzeO
5JA26ELGEP9jOTUzgDtUxkNpCfCdAbmiPkje912R6thFZ2sKMJt/v+dqarO+bK0l
63UiTK6X+y6J5/3Kx9El5Oe4BJMZLi55jVQz8ggP/0ZoJpJCzRSZ84ixAoGATvpR
mJq/Mtb8RYfADIW99avk0FE1QhdNZJ9CiRVt2dc9iA9lwy/jxmMe0RLDESeFg7zF
6e+U3izF5ickHpj+MQktSRyd9koa3MGJmbPnnG4cptCVxlfOfIciJTeIWaPlVTdK
AT2xb86chdjR8pV4wWReL0tUVPdu7crK3lJiFDUCgYEAnhA+GDd702WFpEcDKC7l
vCdhXsJMVxFiohdpUyOKUtonUXTW9xSiq68HCk3FNf8Ln5UvE9MzArGM8RD2FGcY
ejMi6zJ6dgsKPOM9yd8+UxH2N6lRjE7CW41aky6LpM7x2wNv4pXRNmN/hy/xnKaf
GVDno8GjQjVEXzCOVNbso+A=
-----END PRIVATE KEY-----
"""
BOB_PUBLIC_RSA = b"""
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsi29YaE2m8IU6ui05Nh3
7dnvfFs09B5fP/Q3R294NW1X7vd92ojKISA0qFFzTITzdQdse38L/GopI1QcAqgj
dzm7ixbUWAI+7NK/CyZgaLd4AuM/w9Uny84NhWyZJS1tGw5dNiyZNB6q6NWPBza2
4Ys2mHc1Th2kEBaB7QziKa1mWxDx0HqEkgE5H0l71JriaGCtIEUVdnV6AzfmWy0W
PKoVKIXXzLDA2awukS/36fDerS6QWDXc8R1b2VWtCbqsh4n3/JL0mEB738WNituf
3PPNbkRFAYh97S3k0ul9uUnpoYoHdW+tgLqWI46ypB1oPfx0oCYIiWPoSgiFg5yZ
lwIDAQAB
-----END PUBLIC KEY-----
"""


class MockedContext(BaseClientContext):

    def __init__(self, expected_send=[], to_recv=[]):
        self.expected_send = list(reversed(expected_send))
        self.to_recv = list(reversed(to_recv))

    async def recv(self):
        assert self.to_recv, 'No more message should be received'
        return self.to_recv.pop()

    async def send(self, body):
        assert self.expected_send, 'Unexpected message %s (no more should be send)' % body
        expected = self.expected_send.pop()
        assert json.loads(body) == json.loads(expected)


async def bootstrap_PostgreSQLPubKeyService(request, event_loop):
    can_side_effect_or_skip()
    module, url = await init_or_skiptest_parsec_postgresql()

    server = BaseServer()
    server.register_service(module.PostgreSQLService(url))
    msg_svc = module.PostgreSQLPubKeyService()
    server.register_service(msg_svc)
    await server.bootstrap_services()

    def finalize():
        event_loop.run_until_complete(server.teardown_services())

    request.addfinalizer(finalize)
    return msg_svc


@pytest.fixture(params=[InMemoryPubKeyService, bootstrap_PostgreSQLPubKeyService],
                ids=['in_memory', 'postgresql'])
def pubkey_svc(request, event_loop):
    if asyncio.iscoroutinefunction(request.param):
        return event_loop.run_until_complete(request.param(request, event_loop))
    else:
        return request.param()


@pytest.fixture
@pytest.mark.asyncio
async def alice(pubkey_svc):
    await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)
    return {
        'id': 'alice',
        'private_key': RSACipher(key_pem=ALICE_PRIVATE_RSA, public_key=False),
        'public_key': RSACipher(key_pem=ALICE_PUBLIC_RSA, public_key=True)
    }


class TestPubKeyService:

    @pytest.mark.asyncio
    async def test_add_and_get(self, pubkey_svc):
        await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)
        key = await pubkey_svc.get_pubkey('alice', raw=True)
        assert key == ALICE_PUBLIC_RSA
        key = await pubkey_svc.get_pubkey('alice')
        assert isinstance(key, RSACipher)

    @pytest.mark.asyncio
    async def test_multiple_add(self, pubkey_svc):
        await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)
        with pytest.raises(PubKeyError):
            await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)

    @pytest.mark.asyncio
    async def test_get_missing(self, pubkey_svc):
        with pytest.raises(PubKeyNotFound):
            await pubkey_svc.get_pubkey('alice')

    @pytest.mark.asyncio
    async def test_handshake(self, pubkey_svc, alice):
        with patch('parsec.backend.pubkey_service._generate_challenge',
                   new=lambda: "DUMMY_CHALLENGE"):
            expected_send = [
                '{"handshake": "challenge", "challenge": "DUMMY_CHALLENGE"}',
                '{"status": "ok", "handshake": "done"}',
            ]
            answer = "TCk+xUcHNVOFWLE4W0M1vSW1+J+0Mlw0R5Yw1gLc5v6NIsZ0Ghxyo1J4X0lwEEuZxlzmQzrAxxj3V"
            answer += "wJPFEBz42T8k5QtkVBFe+FfZcGwQcN3/hjxDM7TsVHESARjOlmCvkLYHfuZbQg/OSAUlW8tfGbMQ"
            answer += "1/CLgZG1QUptBZKnrJw9EIOHsj8lc8tpWcEO0iYlLKH/fEv0/TiUZG6oFViEKL2duYjQs/5EvmAM"
            answer += "jjDPPhsDLCTPcSHFg82d/bYeAOmbfbE/rXp6Yr6OeiY+5NqRQ4fKj6MXfjBwbo2r5aP6ZFTOaNYl"
            answer += "iHaPa2ixZXtY4lTUvBvvXxXlg7y/3YwK3zCyA=="
            to_recv = [
                '{"handshake": "answer", "identity": "alice", '
                '"answer": "' + answer + '"}'
            ]
            context = MockedContext(expected_send=expected_send, to_recv=to_recv)
            session = await pubkey_svc.handshake(context)
            assert isinstance(session, AuthSession)
            assert session.identity == 'alice'

    # TODO: test bad handshake as well


class TestPubKeyServiceAPI:
    @pytest.mark.asyncio
    async def test_get(self, pubkey_svc):
        await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)

        cmd = {'cmd': 'pubkey_get', 'id': 'alice'}
        ret = await pubkey_svc.dispatch_msg(cmd)
        assert ret['status'] == 'ok'

    @pytest.mark.asyncio
    @pytest.mark.parametrize('bad_cmd', [
        {'cmd': 'pubkey_get', 'id': None},
        {'cmd': 'pubkey_get', 'id': 42},
        {'cmd': 'pubkey_get', 'id': 'alice', 'dummy': 'field'},
        {'cmd': 'pubkey_get'},
        {}])
    async def test_bad_get(self, pubkey_svc, bad_cmd):
        await pubkey_svc.add_pubkey('alice', ALICE_PUBLIC_RSA)

        ret = await pubkey_svc.dispatch_msg(bad_cmd)
        assert ret['status'] == 'bad_msg'
