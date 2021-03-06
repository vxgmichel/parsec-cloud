import pytest
from pendulum import Pendulum

from parsec.trustchain import (
    TrustChainError,
    certified_extract_parts,
    certify_user,
    validate_payload_certified_user,
    certify_device,
    validate_payload_certified_device,
    MAX_TS_BALLPARK,
)

from tests.common import freeze_time


def test_bad_certified_extract_parts():
    with pytest.raises(TrustChainError):
        certified_extract_parts(b"")


def test_certify_user(alice, mallory):
    now = Pendulum(2000, 1, 1)

    with freeze_time(now):
        certification = certify_user(
            alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key
        )
    assert isinstance(certification, bytes)

    certifier, payload = certified_extract_parts(certification)
    assert certifier == alice.device_id
    assert isinstance(payload, bytes)

    data = validate_payload_certified_user(alice.verify_key, payload, now)
    assert data == {
        "type": "user",
        "user_id": mallory.user_id,
        "public_key": mallory.public_key,
        "timestamp": now,
    }


def test_validate_bad_certified_user(alice):
    now = Pendulum(2000, 1, 1)
    with pytest.raises(TrustChainError):
        validate_payload_certified_user(alice.verify_key, b"", now)


def test_validate_certified_user_bad_certifier(alice, bob, mallory):
    now = Pendulum(2000, 1, 1)
    certification = certify_user(
        alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key
    )
    certifier, payload = certified_extract_parts(certification)

    with pytest.raises(TrustChainError):
        validate_payload_certified_user(bob.verify_key, payload, now)


def test_validate_certified_user_too_old(alice, bob, mallory):
    now = Pendulum(2000, 1, 1)
    with freeze_time(now):
        certification = certify_user(
            alice.device_id, alice.signing_key, mallory.user_id, mallory.public_key
        )

    certifier, payload = certified_extract_parts(certification)
    now = now.add(seconds=MAX_TS_BALLPARK + 1)
    with pytest.raises(TrustChainError):
        validate_payload_certified_user(alice.verify_key, payload, now)


def test_certify_device(alice, mallory):
    now = Pendulum(2000, 1, 1)

    with freeze_time(now):
        certification = certify_device(
            alice.device_id, alice.signing_key, mallory.device_id, mallory.verify_key
        )
    assert isinstance(certification, bytes)

    certifier, payload = certified_extract_parts(certification)
    assert certifier == alice.device_id
    assert isinstance(payload, bytes)

    data = validate_payload_certified_device(alice.verify_key, payload, now)
    assert data == {
        "type": "device",
        "device_id": mallory.device_id,
        "verify_key": mallory.verify_key,
        "timestamp": now,
    }


def test_validate_bad_certified_device(alice):
    now = Pendulum(2000, 1, 1)
    with pytest.raises(TrustChainError):
        validate_payload_certified_device(alice.verify_key, b"", now)


def test_validate_certified_device_bad_certifier(alice, bob, mallory):
    now = Pendulum(2000, 1, 1)
    certification = certify_device(
        alice.device_id, alice.signing_key, mallory.device_id, mallory.signing_key
    )
    certifier, payload = certified_extract_parts(certification)

    with pytest.raises(TrustChainError):
        validate_payload_certified_device(bob.verify_key, payload, now)


def test_validate_certified_device_too_old(alice, bob, mallory):
    now = Pendulum(2000, 1, 1)
    with freeze_time(now):
        certification = certify_device(
            alice.device_id, alice.signing_key, mallory.device_id, mallory.signing_key
        )

    certifier, payload = certified_extract_parts(certification)
    now = now.add(seconds=MAX_TS_BALLPARK + 1)
    with pytest.raises(TrustChainError):
        validate_payload_certified_device(alice.verify_key, payload, now)
