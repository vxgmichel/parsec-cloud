import pytest
import trio

from parsec.utils import to_jsonb64


async def populate_backend_vlob(backend):
    await backend.vlob.create("1", "<1 rts>", "<1 wts>", b"1 blob v1")
    await backend.vlob.update("1", "<1 wts>", 2, b"1 blob v2")
    await backend.vlob.create("2", "<2 rts>", "<2 wts>", b"2 blob v1")


def _get_existing_vlob(backend):
    # Backend must have been populated before that
    id, block = list(backend.test_populate_data["vlobs"].items())[0]
    return id, block["rts"], block["wts"], block["blobs"]


@pytest.mark.trio
async def test_vlob_create_and_read(alice_backend_sock, bob_backend_sock):
    blob = to_jsonb64(b"Initial commit.")

    payload = {"id": "123", "rts": "<123 rts>", "wts": "<123 wts>", "blob": blob}
    await alice_backend_sock.send({"cmd": "vlob_create", **payload})
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok"}

    await bob_backend_sock.send({"cmd": "vlob_read", "id": "123", "rts": "<123 rts>"})
    rep = await bob_backend_sock.recv()
    assert rep == {"status": "ok", "id": "123", "version": 1, "blob": blob}


@pytest.mark.parametrize(
    "bad_msg",
    [
        {"id": "123", "wts": "<123 wts>", "blob": to_jsonb64(b"...")},
        {"id": "123", "rts": "<123 rts>", "blob": to_jsonb64(b"...")},
        {"id": "123", "rts": 42, "wts": "<123 wts>", "blob": to_jsonb64(b"...")},
        {"id": "123", "rts": "<123 rts>", "wts": 42, "blob": to_jsonb64(b"...")},
        {"rts": "<123 rts>", "wts": "<123 wts>", "blob": to_jsonb64(b"..."), "bad_field": "foo"},
        {"rts": "<123 rts>", "wts": "<123 wts>", "blob": 42},
        {"rts": "<123 rts>", "wts": "<123 wts>", "blob": None},
        {"id": 42, "rts": "<123 rts>", "wts": "<123 wts>", "blob": to_jsonb64(b"...")},
        {
            "id": "",
            "rts": "<123 rts>",
            "wts": "<123 wts>",
            "blob": to_jsonb64(b"..."),
        },  # Id is 1 long min
        {
            "id": "X" * 33,
            "rts": "<123 rts>",
            "wts": "<123 wts>",
            "blob": to_jsonb64(b"..."),
        },  # Id is 32 long max
    ],
)
@pytest.mark.trio
async def test_vlob_create_bad_msg(alice_backend_sock, bad_msg):
    await alice_backend_sock.send({"cmd": "vlob_create", **bad_msg})
    rep = await alice_backend_sock.recv()
    assert rep["status"] == "bad_message"


@pytest.mark.trio
async def test_vlob_read_not_found(alice_backend_sock):
    await alice_backend_sock.send({"cmd": "vlob_read", "id": "1234", "rts": "TS4242"})
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "not_found_error", "reason": "Vlob not found."}


@pytest.mark.trio
async def test_vlob_read_ok(backend, alice_backend_sock):
    await populate_backend_vlob(backend)

    await alice_backend_sock.send({"cmd": "vlob_read", "id": "1", "rts": "<1 rts>"})
    rep = await alice_backend_sock.recv()

    assert rep == {"status": "ok", "id": "1", "blob": to_jsonb64(b"1 blob v2"), "version": 2}


@pytest.mark.parametrize(
    "bad_msg",
    [
        {"id": "1234", "rts": "TS4242", "bad_field": "foo"},
        {"id": "1234"},
        {"id": "1234", "rts": 42},
        {"id": "1234", "rts": None},
        {"id": 42, "rts": "TS4242"},
        {"id": None, "rts": "TS4242"},
        # {'id': '1234567890', 'rts': 'TS4242'},  # TODO bad?
        {},
    ],
)
@pytest.mark.trio
async def test_vlob_read_bad_msg(alice_backend_sock, bad_msg):
    await alice_backend_sock.send({"cmd": "vlob_read", **bad_msg})
    rep = await alice_backend_sock.recv()
    # Id and trust_seed are invalid anyway, but here we test another layer
    # so it's not important as long as we get our `bad_message` status
    assert rep["status"] == "bad_message"


@pytest.mark.trio
async def test_read_bad_version(backend, alice_backend_sock):
    await populate_backend_vlob(backend)

    await alice_backend_sock.send({"cmd": "vlob_read", "id": "1", "rts": "<1 rts>", "version": 3})
    rep = await alice_backend_sock.recv()

    assert rep == {"status": "version_error", "reason": "Wrong blob version."}


@pytest.mark.trio
async def test_vlob_update_ok(backend, alice_backend_sock):
    await populate_backend_vlob(backend)

    await alice_backend_sock.send(
        {
            "cmd": "vlob_update",
            "id": "1",
            "wts": "<1 wts>",
            "version": 3,
            "blob": to_jsonb64(b"Next version."),
        }
    )
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok"}


@pytest.mark.trio
async def test_vlob_update_not_found(alice_backend_sock):
    await alice_backend_sock.send(
        {
            "cmd": "vlob_update",
            "id": "123",
            "wts": "WTS42",
            "version": 2,
            "blob": to_jsonb64(b"Next version."),
        }
    )
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "not_found_error", "reason": "Vlob not found."}


@pytest.mark.parametrize(
    "bad_msg",
    [
        {
            "id": "1234",
            "wts": "WTS42",
            "version": "42",
            "blob": to_jsonb64(b"..."),
            "bad_field": "foo",
        },
        {"id": "1234", "wts": "WTS42", "version": "42", "blob": None},
        {"id": "1234", "wts": "WTS42", "version": "42", "blob": 42},
        {"id": "1234", "wts": "WTS42", "version": "42"},
        {"id": "1234", "wts": "WTS42", "version": None, "blob": to_jsonb64(b"...")},
        {"id": "1234", "wts": "WTS42", "version": -1, "blob": to_jsonb64(b"...")},
        {"id": "1234", "wts": None, "version": "42", "blob": to_jsonb64(b"...")},
        {"id": "1234", "wts": 42, "version": "42", "blob": to_jsonb64(b"...")},
        {"id": "1234", "version": "42", "blob": to_jsonb64(b"...")},
        {"id": 42, "wts": "WTS42", "version": "42", "blob": to_jsonb64(b"...")},
        {"id": None, "wts": "WTS42", "version": "42", "blob": to_jsonb64(b"...")},
        {"wts": "WTS42", "version": "42", "blob": to_jsonb64(b"...")},
        {},
    ],
)
@pytest.mark.trio
async def test_vlob_update_bad_msg(alice_backend_sock, bad_msg):
    await alice_backend_sock.send({"cmd": "vlob_update", **bad_msg})
    rep = await alice_backend_sock.recv()
    # Id and wts are invalid anyway, but here we test another layer
    # so it's not important as long as we get our `bad_message` status
    assert rep["status"] == "bad_message"


@pytest.mark.trio
async def test_update_bad_version(backend, alice_backend_sock):
    await populate_backend_vlob(backend)

    await alice_backend_sock.send(
        {
            "cmd": "vlob_update",
            "id": "1",
            "wts": "<1 wts>",
            "version": 4,
            "blob": to_jsonb64(b"Next version."),
        }
    )
    rep = await alice_backend_sock.recv()

    assert rep == {"status": "version_error", "reason": "Wrong blob version."}


@pytest.mark.trio
async def test_update_bad_seed(backend, alice_backend_sock):
    await populate_backend_vlob(backend)

    await alice_backend_sock.send(
        {
            "cmd": "vlob_update",
            "id": "1",
            "wts": "dummy_seed",
            "version": 3,
            "blob": to_jsonb64(b"Next version."),
        }
    )
    rep = await alice_backend_sock.recv()

    assert rep == {"status": "trust_seed_error", "reason": "Invalid write trust seed."}


@pytest.mark.trio
async def test_update_get_event(backend, alice, alice_backend_sock, bob_backend_sock):
    await populate_backend_vlob(backend)

    async def update_vlob(id, version):
        await alice_backend_sock.send(
            {
                "cmd": "vlob_update",
                "id": id,
                "wts": "<%s wts>" % id,
                "version": version,
                "blob": to_jsonb64(b""),
            }
        )
        rep = await alice_backend_sock.recv()
        assert rep == {"status": "ok"}

    # Update before event registration should be ignored
    await update_vlob("1", 3)

    # Register to event
    await bob_backend_sock.send({"cmd": "event_subscribe", "event": "vlob_updated", "subject": "1"})
    rep = await bob_backend_sock.recv()
    assert rep == {"status": "ok"}

    # Multiple events should stack up
    await update_vlob("1", 4)
    await update_vlob("1", 5)

    # Unrelated event should be ignored
    await update_vlob("2", 2)

    # Time to retrieve the events...
    async def get_event():
        await bob_backend_sock.send({"cmd": "event_listen", "wait": False})
        return await bob_backend_sock.recv()

    e1 = await get_event()
    assert e1 == {"status": "ok", "sender": alice.id, "event": "vlob_updated", "subject": "1"}
    e2 = await get_event()
    assert e2 == {"status": "ok", "sender": alice.id, "event": "vlob_updated", "subject": "1"}
    e3 = await get_event()
    assert e3 == {"status": "no_events"}  # No more events

    # Test the waiting mode as well
    await bob_backend_sock.send({"cmd": "event_listen", "wait": True})
    await update_vlob("1", 6)
    with trio.fail_after(1):
        rep = await bob_backend_sock.recv()
    assert rep == {
        "status": "ok",
        "sender": alice.id,
        "event": "vlob_updated",
        "status": "ok",
        "subject": "1",
    }


# TODO
@pytest.mark.xfail
@pytest.mark.trio
async def test_update_with_notify_beacons(backend, alice, alice_backend_sock, bob_backend_sock):
    await populate_backend_vlob(backend)

    # Register to event
    await bob_backend_sock.send(
        {"cmd": "event_subscribe", "event": "vlob_updated", "subject": "42"}
    )
    rep = await bob_backend_sock.recv()
    assert rep == {"status": "ok"}
    await bob_backend_sock.send(
        {"cmd": "event_subscribe", "event": "vlob_updated", "subject": "44"}
    )
    rep = await bob_backend_sock.recv()
    assert rep == {"status": "ok"}

    await alice_backend_sock.send(
        {
            "cmd": "vlob_update",
            "id": "1",
            "wts": "<1 wts>",
            "version": 3,
            "blob": to_jsonb64(b""),
            "notify_beacons": ["42", "43", "44"],
        }
    )
    rep = await alice_backend_sock.recv()
    assert rep == {"status": "ok"}

    # Time to retrieve the events...
    async def get_event():
        await bob_backend_sock.send({"cmd": "event_listen", "wait": False})
        return await bob_backend_sock.recv()

    e1 = await get_event()
    assert e1 == {"status": "ok", "event": "vlob_updated", "status": "ok", "subject": "42"}
    e2 = await get_event()
    assert e2 == {"status": "ok", "event": "vlob_updated", "status": "ok", "subject": "44"}
    e3 = await get_event()
    assert e3 == {"status": "no_events"}  # No more events

    # Fetch the sink data...
    async def get_sink_vlob(id):
        await bob_backend_sock.send({"cmd": "vlob_read", "id": id, "rts": id})
        return await bob_backend_sock.recv()

    rep = await get_sink_vlob("42")
    assert rep == {"id": "42", "status": "ok", "version": 1, "blob": to_jsonb64(b"1")}
    rep = await get_sink_vlob("43")
    assert rep == {"id": "43", "status": "ok", "version": 1, "blob": to_jsonb64(b"1")}
    rep = await get_sink_vlob("44")
    assert rep == {"id": "44", "status": "ok", "version": 1, "blob": to_jsonb64(b"1")}
