import pytest
import trio
import pendulum

from parsec.trustchain import certify_user, certify_device
from parsec.backend.user import INVITATION_VALIDITY
from parsec.api.protocole import user_create_serializer

from tests.common import freeze_time
from tests.backend.user.test_access import user_get


async def user_create(sock, **kwargs):
    await sock.send(user_create_serializer.req_dumps({"cmd": "user_create", **kwargs}))
    raw_rep = await sock.recv()
    rep = user_create_serializer.rep_loads(raw_rep)
    return rep


@pytest.mark.trio
@pytest.mark.parametrize("is_admin", [True, False])
async def test_user_create_ok(
    backend, backend_sock_factory, alice_backend_sock, alice, mallory, is_admin
):
    now = pendulum.now()
    certified_user = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key, now=now
    )
    certified_device = certify_device(
        alice.device_id, alice.signing_key, mallory.device_id, mallory.verify_key, now=now
    )

    with backend.event_bus.listen() as spy:
        rep = await user_create(
            alice_backend_sock,
            certified_user=certified_user,
            certified_device=certified_device,
            is_admin=is_admin,
        )
        assert rep == {"status": "ok"}

        with trio.fail_after(1):
            # No guarantees this event occurs before the command's return
            await spy.wait("user.created", kwargs={"user_id": mallory.user_id})

    # Make sure mallory can connect now
    async with backend_sock_factory(backend, mallory) as sock:
        rep = await user_get(sock, user_id=mallory.user_id)
        assert rep["status"] == "ok"
        assert rep["is_admin"] == is_admin


@pytest.mark.trio
async def test_user_create_invalid_certified(alice_backend_sock, alice, bob, mallory):
    now = pendulum.now()
    good_certified_user = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key, now=now
    )
    good_certified_device = certify_device(
        alice.device_id, alice.signing_key, mallory.device_id, mallory.verify_key, now=now
    )
    bad_certified_user = certify_user(
        bob.device_id, bob.signing_key, mallory.user_id, mallory.public_key, now=now
    )
    bad_certified_device = certify_device(
        bob.device_id, bob.signing_key, mallory.device_id, mallory.verify_key, now=now
    )

    for cu, cd in [
        (good_certified_user, bad_certified_device),
        (bad_certified_user, good_certified_device),
        (bad_certified_user, bad_certified_device),
    ]:
        rep = await user_create(alice_backend_sock, certified_user=cu, certified_device=cd)
        assert rep == {
            "status": "invalid_certification",
            "reason": "Certifier is not the authenticated device.",
        }


@pytest.mark.trio
async def test_user_create_not_matching_user_device(alice_backend_sock, alice, mallory):
    now = pendulum.now()
    certified_user = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key, now=now
    )
    certified_device = certify_device(
        alice.device_id, alice.signing_key, "zack@foo", mallory.verify_key, now=now
    )

    rep = await user_create(
        alice_backend_sock, certified_user=certified_user, certified_device=certified_device
    )
    assert rep == {
        "status": "invalid_data",
        "reason": "Device and User must have the same user ID.",
    }


@pytest.mark.trio
async def test_user_create_already_exists(alice_backend_sock, alice, bob):
    now = pendulum.now()
    certified_user = certify_user(
        alice.device_id, alice.signing_key, bob.user_id, bob.public_key, now=now
    )
    certified_device = certify_device(
        alice.device_id, alice.signing_key, bob.device_id, bob.verify_key, now=now
    )

    rep = await user_create(
        alice_backend_sock, certified_user=certified_user, certified_device=certified_device
    )
    assert rep == {"status": "already_exists", "reason": "User `bob` already exists"}


@pytest.mark.trio
async def test_user_create_certify_too_old(alice_backend_sock, alice, mallory):
    too_old = pendulum.Pendulum(2000, 1, 1)
    now = too_old.add(seconds=INVITATION_VALIDITY + 1)
    good_certified_user = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key, now=now
    )
    good_certified_device = certify_device(
        alice.device_id, alice.signing_key, mallory.device_id, mallory.verify_key, now=now
    )
    bad_certified_user = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key, now=too_old
    )
    bad_certified_device = certify_device(
        alice.device_id, alice.signing_key, mallory.device_id, mallory.verify_key, now=too_old
    )

    with freeze_time(now):
        for cu, cd in [
            (good_certified_user, bad_certified_device),
            (bad_certified_user, good_certified_device),
            (bad_certified_user, bad_certified_device),
        ]:
            rep = await user_create(alice_backend_sock, certified_user=cu, certified_device=cd)
            assert rep == {
                "status": "invalid_certification",
                "reason": "Invalid certification data (Timestamp is too old.).",
            }
