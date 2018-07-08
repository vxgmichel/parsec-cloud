import pytest


@pytest.mark.trio
async def test_beacon_read_any(alice_backend_sock):
    await alice_backend_sock.send({"cmd": "beacon_read", "id": "123", "from_index": 0})
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok", "id": "123", "data": [], "data_count": 0}


@pytest.mark.trio
async def test_beacon_multimessages(backend, alice_backend_sock):
    await backend.beacon.update("1", data="a1", author="alice")
    await backend.beacon.update("1", data="b2", author="bob")
    await backend.beacon.update("1", data="b3", author="bob")

    await alice_backend_sock.send({"cmd": "beacon_read", "id": "1", "from_index": 0})
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok", "id": "1", "data": ["a1", "b2", "b3"], "data_count": 3}

    # Also test offset
    await alice_backend_sock.send({"cmd": "beacon_read", "id": "1", "from_index": 2})
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok", "id": "1", "data": ["b3"], "data_count": 1}


@pytest.mark.trio
async def test_beacon_in_vlob_update(backend, alice_backend_sock):
    beacons_ids = ["123", "456", "789"]

    await backend.vlob.create("1", "<1 rts>", "<1 wts>", blob=b"foo")
    await backend.vlob.update("1", "<1 wts>", version=2, blob=b"bar", notify_beacons=beacons_ids)

    for beacon_id in beacons_ids:
        await alice_backend_sock.send({"cmd": "beacon_read", "id": beacon_id, "from_index": 0})
        rep = await alice_backend_sock.recv()
        assert rep == {"status": "ok", "id": beacon_id, "data": ["1"], "data_count": 1}


@pytest.mark.trio
async def test_beacon_in_vlob_create(backend, alice_backend_sock):
    beacons_ids = ["123", "456", "789"]

    await backend.vlob.create("1", "<1 rts>", "<1 wts>", b"foo", notify_beacons=beacons_ids)

    for beacon_id in beacons_ids:
        await alice_backend_sock.send({"cmd": "beacon_read", "id": beacon_id, "from_index": 0})
        rep = await alice_backend_sock.recv()
        assert rep == {"status": "ok", "id": beacon_id, "data": ["1"], "data_count": 1}
