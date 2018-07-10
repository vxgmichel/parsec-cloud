import pytest
import trio
from async_generator import asynccontextmanager

from tests.common import freeze_time
from tests.open_tcp_stream_mock_wrapper import offline


@asynccontextmanager
async def wait_for_entries_synced(core, entries_pathes):
    # # First make sure the backend event manager is ready to listen events
    # await core.backend_events_manager.wait_backend_online()

    event = trio.Event()
    to_sync = set(entries_pathes)
    synced = set()

    core_id = id(core)

    def _on_entry_synced(sender, id, path):
        if event.is_set():
            return

        print(core_id, "ENTRY SYNCED", id, path)

        if path not in to_sync:
            raise AssertionError(f"{path} wasn't supposed to be synced, expected only {to_sync}")
        if path in synced:
            raise AssertionError(
                f"{path} synced two time while waiting synchro for {to_sync - synced}"
            )

        synced.add(path)
        if synced == to_sync:
            event.set()

    print("******************** connect signal to ", id(core.signal_ns), core, entries_pathes)
    with core.signal_ns.signal("fs.entry.synced").temporarily_connected_to(_on_entry_synced):

        yield event

        with trio.fail_after(1.0):
            await event.wait()
    print("+++++++++++++++++ done with", id(core.signal_ns), core, entries_pathes)


@pytest.mark.trio
async def test_online_sync(running_backend, core_factory, alice, alice2):
    # Given the cores are initialized while the backend is online, we are
    # guaranteed they are connected
    alice_core = await core_factory()
    alice2_core2 = await core_factory()
    await alice_core.login(alice)
    await alice2_core2.login(alice2)

    async with wait_for_entries_synced(alice2_core2, ["/"]), wait_for_entries_synced(
        alice_core, ("/", "/foo.txt")
    ):

        with freeze_time("2000-01-02"):
            await alice_core.fs.file_create("/foo.txt")

        with freeze_time("2000-01-03"):
            await alice_core.fs.file_write("/foo.txt", b"hello world !")

        await alice_core.fs.sync("/foo.txt")

    stat = await alice_core.fs.stat("/foo.txt")
    stat2 = await alice2_core2.fs.stat("/foo.txt")
    assert stat2 == stat


@pytest.mark.trio
async def test_sync_then_clean_start(running_backend, core_factory, alice, alice2):
    # Given the cores are initialized while the backend is online, we are
    # guaranteed they are connected
    alice_core = await core_factory()
    await alice_core.login(alice)

    async with wait_for_entries_synced(alice_core, ("/", "/foo.txt")):

        with freeze_time("2000-01-02"):
            await alice_core.fs.file_create("/foo.txt")

        with freeze_time("2000-01-03"):
            await alice_core.fs.file_write("/foo.txt", b"v1")

        await alice_core.fs.sync("/foo.txt")

    alice2_core2 = await core_factory()
    async with wait_for_entries_synced(alice2_core2, ["/"]):
        await alice2_core2.login(alice2)

    for path in ("/", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat


@pytest.mark.trio
async def test_sync_then_fast_forward_on_start(running_backend, core_factory, alice, alice2):
    # Given the cores are initialized while the backend is online, we are
    # guaranteed they are connected
    alice_core = await core_factory()
    alice2_core2 = await core_factory()
    await alice_core.login(alice)
    await alice2_core2.login(alice2)

    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"v1")

    async with wait_for_entries_synced(alice2_core2, ["/"]), wait_for_entries_synced(
        alice_core, ("/", "/foo.txt")
    ):
        await alice_core.fs.sync("/foo.txt")

    await alice2_core2.logout()

    with freeze_time("2000-01-04"):
        await alice_core.fs.file_write("/foo.txt", b"v2")
        await alice_core.fs.folder_create("/bar")

    async with wait_for_entries_synced(alice_core, ["/", "/bar", "/foo.txt"]):
        await alice_core.fs.sync("/")

    async with wait_for_entries_synced(alice2_core2, ["/"]):
        await alice2_core2.login(alice2)

    for path in ("/", "/bar", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat


@pytest.mark.trio
async def test_fast_forward_on_offline_during_sync(
    mock_clock, server_factory, backend, core_factory, alice, alice2
):
    server1 = server_factory(backend.handle_client)
    server2 = server_factory(backend.handle_client)

    # Given the cores are initialized while the backend is online, we are
    # guaranteed they are connected
    alice_core = await core_factory(config={"backend_addr": server1.addr})
    alice2_core2 = await core_factory(config={"backend_addr": server2.addr})
    await alice_core.login(alice)
    await alice2_core2.login(alice2)

    async with wait_for_entries_synced(alice2_core2, ["/"]), wait_for_entries_synced(
        alice_core, ("/", "/foo.txt")
    ):
        with freeze_time("2000-01-02"):
            await alice_core.fs.file_create("/foo.txt")

        with freeze_time("2000-01-03"):
            await alice_core.fs.file_write("/foo.txt", b"v1")

        await alice_core.fs.sync("/foo.txt")

    # core2 goes offline, other core is still connected to backend
    async with wait_for_entries_synced(alice_core, ("/", "/foo.txt")):
        with offline(server1.addr):

            with freeze_time("2000-01-04"):
                await alice2_core2.fs.file_write("/foo.txt", b"v2")
                await alice2_core2.fs.folder_create("/bar")

            async with wait_for_entries_synced(alice2_core2, ("/", "/bar", "/foo.txt")):
                await alice2_core2.fs.sync("/")

        # Wait some time for the sync to kicks in
        mock_clock.jump(5)

    for path in ("/", "/bar", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat
