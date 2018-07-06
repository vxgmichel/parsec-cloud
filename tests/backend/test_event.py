import pytest
import trio

from tests.common import connect_backend


@pytest.mark.trio
async def test_event_subscribe(backend, alice_backend_sock):
    sock = alice_backend_sock

    await sock.send({"cmd": "event_subscribe", "event": "ping", "subject": "foo"})
    rep = await sock.recv()
    assert rep == {"status": "ok"}


@pytest.mark.trio
async def test_event_subscribe_unkown_event(backend, alice_backend_sock):
    sock = alice_backend_sock

    await sock.send({"cmd": "event_subscribe", "event": "foo", "subject": "foo"})
    rep = await sock.recv()
    assert rep == {"status": "bad_message", "errors": {"event": ["Not a valid choice."]}}


async def subscribe(sock, event, subject):
    await sock.send({"cmd": "event_subscribe", "event": event, "subject": subject})
    rep = await sock.recv()
    assert rep == {"status": "ok"}


async def ping(sock, subject):
    await sock.send({"cmd": "ping", "ping": subject})
    rep = await sock.recv()
    assert rep == {"status": "ok", "pong": subject}


@pytest.mark.trio
async def test_event_unsubscribe(backend, alice_backend_sock):
    sock = alice_backend_sock

    await subscribe(sock, "ping", "foo")

    await sock.send({"cmd": "event_unsubscribe", "event": "ping", "subject": "foo"})
    rep = await sock.recv()
    assert rep == {"status": "ok"}


@pytest.mark.trio
async def test_event_unsubscribe_bad_subject(backend, alice_backend_sock):
    sock = alice_backend_sock

    await subscribe(sock, "ping", "foo")
    await sock.send({"cmd": "event_unsubscribe", "event": "ping", "subject": "bar"})
    rep = await sock.recv()
    assert rep == {
        "status": "not_subscribed",
        "reason": "Not subscribed to this event/subject couple",
    }


@pytest.mark.trio
async def test_event_unsubscribe_bad_event(backend, alice_backend_sock):
    sock = alice_backend_sock

    await sock.send({"cmd": "event_unsubscribe", "event": "ping", "subject": "bar"})
    rep = await sock.recv()
    assert rep == {
        "status": "not_subscribed",
        "reason": "Not subscribed to this event/subject couple",
    }


@pytest.mark.trio
async def test_event_unsubscribe_unknown_event(backend, alice_backend_sock):
    sock = alice_backend_sock

    await sock.send({"cmd": "event_unsubscribe", "event": "unknown", "subject": "bar"})
    rep = await sock.recv()
    assert rep == {"status": "bad_message", "errors": {"event": ["Not a valid choice."]}}


@pytest.mark.trio
async def test_ignore_own_events(backend, alice_backend_sock):
    sock = alice_backend_sock

    await subscribe(sock, "ping", "foo")

    await ping(sock, "foo")

    await sock.send({"cmd": "event_listen", "wait": False})
    rep = await sock.recv()
    assert rep == {"status": "no_events"}


@pytest.mark.trio
async def test_event_listen(backend, alice_backend_sock, bob_backend_sock):
    alice_sock, bob_sock = alice_backend_sock, bob_backend_sock

    await alice_sock.send({"cmd": "event_listen", "wait": False})
    rep = await alice_sock.recv()
    assert rep == {"status": "no_events"}

    await subscribe(alice_sock, "ping", "foo")

    await alice_sock.send({"cmd": "event_listen"})

    await ping(bob_sock, "bar")
    await ping(bob_sock, "foo")

    with trio.fail_after(1):
        rep = await alice_sock.recv()
    assert rep == {"status": "ok", "sender": "bob@dev1", "event": "ping", "subject": "foo"}

    await ping(bob_sock, "foo")

    await alice_sock.send({"cmd": "event_listen", "wait": False})
    rep = await alice_sock.recv()
    assert rep == {"status": "ok", "sender": "bob@dev1", "event": "ping", "subject": "foo"}

    await alice_sock.send({"cmd": "event_listen", "wait": False})
    rep = await alice_sock.recv()
    assert rep == {"status": "no_events"}


# TODO: test private events
