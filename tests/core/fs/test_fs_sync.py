import pytest
from trio.testing import wait_all_tasks_blocked

from tests.common import core_factory, freeze_time
from tests.open_tcp_stream_mock_wrapper import offline


@pytest.mark.trio
async def test_online_sync(
    magical_per_app_signals_context, autojump_clock, running_backend, alice_core, alice2_core2
):
    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"hello world !")

    await alice_core.fs.sync("/foo.txt")

    # Wait for core2 to settle down after receiving notification from core1
    await wait_all_tasks_blocked(cushion=0.01)

    stat = await alice_core.fs.stat("/foo.txt")
    stat2 = await alice2_core2.fs.stat("/foo.txt")
    assert stat2 == stat


@pytest.mark.trio
async def test_sync_then_clean_start(
    tmpdir, autojump_clock, alice2, backend_addr, running_backend, alice_core
):
    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"v1")

    await alice_core.fs.sync("/foo.txt")

    async with core_factory(base_settings_path=tmpdir, backend_addr=backend_addr) as alice2_core2:
        await alice2_core2.login(alice2)

        await wait_all_tasks_blocked(cushion=0.01)

        for path in ("/", "/foo.txt"):
            stat = await alice_core.fs.stat(path)
            stat2 = await alice2_core2.fs.stat(path)
            assert stat2 == stat


@pytest.mark.trio
async def test_sync_then_fast_forward_on_start(
    nursery, autojump_clock, alice2, running_backend, alice_core, alice2_core2
):
    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"v1")

    await alice_core.fs.sync("/foo.txt")

    await alice2_core2.logout()

    with freeze_time("2000-01-04"):
        await alice_core.fs.file_write("/foo.txt", b"v2")
        await alice_core.fs.folder_create("/bar")
    await alice_core.fs.sync("/")

    await alice2_core2.login(alice2)

    await wait_all_tasks_blocked(cushion=0.01)

    for path in ("/", "/bar", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat


@pytest.mark.trio
async def test_fast_forward_on_offline_during_sync(
    autojump_clock, backend_addr, running_backend, alice_core, alice2_core2, tcp_stream_spy
):
    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"v1")

    await alice_core.fs.sync("/foo.txt")

    await wait_all_tasks_blocked(cushion=0.01)

    with offline(backend_addr):

        with freeze_time("2000-01-04"):
            await alice2_core2.fs.file_write("/foo.txt", b"v2")
            await alice2_core2.fs.folder_create("/bar")

        await alice2_core2.fs.sync("/")

    await wait_all_tasks_blocked(cushion=0.01)

    for path in ("/", "/bar", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat
