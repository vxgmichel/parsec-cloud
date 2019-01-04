import pytest
import attr
from unittest.mock import ANY
from pendulum import Pendulum

from parsec.types import DeviceID
from parsec.crypto import SigningKey, PrivateKey
from parsec.trustchain import certify_user, certify_device, certify_device_revocation
from parsec.api.protocole import packb, user_get_serializer, user_find_serializer
from parsec.backend.user import (
    User as BackendUser,
    Device as BackendDevice,
    DevicesMapping as BackendDevicesMapping,
)

from tests.common import freeze_time


async def user_get(sock, user_id):
    await sock.send(user_get_serializer.req_dumps({"cmd": "user_get", "user_id": user_id}))
    raw_rep = await sock.recv()
    return user_get_serializer.rep_loads(raw_rep)


async def user_find(sock, **kwargs):
    await sock.send(user_find_serializer.req_dumps({"cmd": "user_find", **kwargs}))
    raw_rep = await sock.recv()
    return user_find_serializer.rep_loads(raw_rep)


@pytest.mark.trio
async def test_api_user_get_ok(backend, alice_backend_sock, bob):
    rep = await user_get(alice_backend_sock, bob.user_id)
    assert rep == {
        "status": "ok",
        "is_admin": False,
        "user_id": bob.user_id,
        "certified_user": ANY,
        "user_certifier": None,
        "created_on": Pendulum(2000, 1, 1),
        "devices": {
            bob.device_name: {
                "device_id": bob.device_id,
                "created_on": Pendulum(2000, 1, 1),
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": None,
            }
        },
        "trustchain": {},
    }


@attr.s
class Device:
    device_id = attr.ib()
    signing_key = attr.ib()


def user_factory(creator, device_id):
    device_id = DeviceID(device_id)
    private_key = PrivateKey.generate()
    certified_user = certify_user(
        creator.device_id, creator.signing_key, device_id.user_id, private_key.public_key
    )
    local_device, backend_device = device_factory(creator, device_id)
    backend_user = BackendUser(
        device_id.user_id,
        certified_user=certified_user,
        user_certifier=creator.device_id,
        devices=BackendDevicesMapping(backend_device),
    )
    return local_device, backend_user


def device_factory(creator, device_id):
    device_id = DeviceID(device_id)
    signing_key = SigningKey.generate()
    certified_device = certify_device(
        creator.device_id, creator.signing_key, device_id, signing_key.verify_key
    )
    backend_device = BackendDevice(
        device_id=device_id, certified_device=certified_device, device_certifier=creator.device_id
    )
    local_device = Device(device_id=device_id, signing_key=signing_key)
    return local_device, backend_device


async def create_user(backend, creator, device_id):
    local_device, user = user_factory(creator, device_id)
    await backend.user.create_user(user)
    return local_device


async def create_device(backend, creator, device_id):
    local_device, device = device_factory(creator, device_id)
    await backend.user.create_device(device)
    return local_device


async def revoke_device(backend, revoker, device_id):
    certified_revocation = certify_device_revocation(
        revoker.device_id, revoker.signing_key, device_id
    )
    await backend.user.revoke_device(device_id, certified_revocation, revoker.device_id)


@pytest.mark.trio
async def test_api_user_get_ok_deep_trustchain(backend, alice_backend_sock, alice):
    # <root> --> alice@dev1 --> roger@dev1 --> mike@dev1 --> mike@dev2
    #                       --> philippe@dev1 --> philippe@dev2
    d1 = Pendulum(2000, 1, 1)
    d2 = Pendulum(2000, 1, 2)

    with freeze_time(d1):
        roger1 = await create_user(backend, alice, "roger@dev1")
        mike1 = await create_user(backend, roger1, "mike@dev1")
        mike2 = await create_device(backend, mike1, "mike@dev2")
        ph1 = await create_user(backend, alice, "philippe@dev1")
        ph2 = await create_device(backend, ph1, "philippe@dev2")

    with freeze_time(d2):
        await revoke_device(backend, ph1, roger1.device_id)
        await revoke_device(backend, ph2, mike2.device_id)

    rep = await user_get(alice_backend_sock, mike2.device_id.user_id)
    assert rep == {
        "status": "ok",
        "is_admin": False,
        "user_id": mike2.device_id.user_id,
        "certified_user": ANY,
        "user_certifier": roger1.device_id,
        "created_on": d1,
        "devices": {
            mike1.device_id.device_name: {
                "device_id": mike1.device_id,
                "created_on": d1,
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": roger1.device_id,
            },
            mike2.device_id.device_name: {
                "device_id": mike2.device_id,
                "created_on": d1,
                "revocated_on": d2,
                "certified_revocation": ANY,
                "revocation_certifier": ph2.device_id,
                "certified_device": ANY,
                "device_certifier": mike1.device_id,
            },
        },
        "trustchain": {
            alice.device_id: {
                "device_id": alice.device_id,
                "created_on": d1,
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": None,
            },
            mike1.device_id: {
                "device_id": mike1.device_id,
                "created_on": d1,
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": roger1.device_id,
            },
            roger1.device_id: {
                "device_id": roger1.device_id,
                "created_on": d1,
                "revocated_on": d2,
                "certified_revocation": ANY,
                "revocation_certifier": ph1.device_id,
                "certified_device": ANY,
                "device_certifier": alice.device_id,
            },
            ph1.device_id: {
                "device_id": ph1.device_id,
                "created_on": d1,
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": alice.device_id,
            },
            ph2.device_id: {
                "device_id": ph2.device_id,
                "created_on": d1,
                "revocated_on": None,
                "certified_revocation": None,
                "revocation_certifier": None,
                "certified_device": ANY,
                "device_certifier": ph1.device_id,
            },
        },
    }


@pytest.mark.parametrize(
    "bad_msg", [{"user_id": 42}, {"user_id": None}, {"user_id": "alice", "unknown": "field"}, {}]
)
@pytest.mark.trio
async def test_api_user_get_bad_msg(alice_backend_sock, bad_msg):
    await alice_backend_sock.send(packb({"cmd": "user_get", **bad_msg}))
    raw_rep = await alice_backend_sock.recv()
    rep = user_get_serializer.rep_loads(raw_rep)
    assert rep["status"] == "bad_message"


@pytest.mark.trio
async def test_api_user_get_not_found(alice_backend_sock):
    rep = await user_get(alice_backend_sock, "dummy")
    assert rep == {"status": "not_found"}


@pytest.mark.trio
async def test_api_user_find(alice, backend, alice_backend_sock):
    # Populate with cool guys
    await create_user(backend, alice, "Philippe@p1")
    await create_device(backend, alice, "Philippe@p2")
    await create_user(backend, alice, "Mike@p1")
    await create_user(backend, alice, "Blacky@p1")
    await create_user(backend, alice, "Philip_J_Fry@p1")

    # Test exact match
    rep = await user_find(alice_backend_sock, query="Mike")
    assert rep == {"status": "ok", "results": ["Mike"], "per_page": 100, "page": 1, "total": 1}

    # Test partial search
    rep = await user_find(alice_backend_sock, query="Phil")
    assert rep == {
        "status": "ok",
        "results": ["Philip_J_Fry", "Philippe"],
        "per_page": 100,
        "page": 1,
        "total": 2,
    }

    # Test pagination
    rep = await user_find(alice_backend_sock, query="Phil", page=1, per_page=1)
    assert rep == {
        "status": "ok",
        "results": ["Philip_J_Fry"],
        "per_page": 1,
        "page": 1,
        "total": 2,
    }

    # Test out of pagination
    rep = await user_find(alice_backend_sock, query="Phil", page=2, per_page=5)
    assert rep == {"status": "ok", "results": [], "per_page": 5, "page": 2, "total": 2}

    # Test no params
    rep = await user_find(alice_backend_sock)
    assert rep == {
        "status": "ok",
        "results": ["alice", "Blacky", "bob", "Mike", "Philip_J_Fry", "Philippe"],
        "per_page": 100,
        "page": 1,
        "total": 6,
    }

    # Test bad params
    for bad in [{"dummy": 42}, {"query": 42}, {"page": 0}, {"per_page": 0}, {"per_page": 101}]:
        await alice_backend_sock.send(packb({"cmd": "user_find", **bad}))
        raw_rep = await alice_backend_sock.recv()
        rep = user_find_serializer.rep_loads(raw_rep)
        assert rep["status"] == "bad_message"
