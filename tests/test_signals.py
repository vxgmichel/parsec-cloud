import pytest
from trio.testing import wait_all_tasks_blocked

from parsec.signals import get_signal

from tests.common import connect_signal_as_event


@pytest.mark.trio
async def test_backend_signal_ns_not_leaking(running_backend, signal_ns):
    backend = running_backend.backend

    ping_in_core = connect_signal_as_event(signal_ns, "ping")
    pong_in_backend = connect_signal_as_event(backend.signal_ns, "pong")

    signal_ns.signal("pong").send("core")
    backend.signal_ns.signal("ping").send("backend")

    await wait_all_tasks_blocked(cushion=0.01)

    assert not ping_in_core.is_set()
    assert not pong_in_backend.is_set()


@pytest.mark.trio
async def test_signal_ns_is_pushed(signal_ns):
    global_ping = get_signal("ping")
    signal_ns_ping = signal_ns.signal("ping")

    assert global_ping is signal_ns_ping
