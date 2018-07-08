import pytest
import trio
from async_generator import asynccontextmanager
from trio.testing import wait_all_tasks_blocked

from tests.common import core_factory, freeze_time, connect_signal_as_event
from tests.open_tcp_stream_mock_wrapper import offline


@asynccontextmanager
async def wait_for_entries_synced(core, entries_pathes):
    # First make sure the backend event manager is ready to listen events
    await core.backend_events_manager.wait_backend_online()
    print(
        "subs:",
        id(core),
        core.backend_events_manager._subscribed_events,
        core.backend_events_manager._subscribed_events_changed.is_set(),
    )

    event = trio.Event()
    to_sync = set(entries_pathes)
    synced = set()

    core_id = id(core)

    def _on_entry_synced(sender, id, path):
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

    print("******************** connect signal to ", id(core.signal_ns))
    with core.signal_ns.signal("fs.entry.synced").temporarily_connected_to(_on_entry_synced):

        yield event

        await event.wait()


@pytest.mark.trio
async def test_online_sync(running_backend, alice_core, alice2_core2):
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
async def test_sync_then_clean_start(
    tmpdir, autojump_clock, alice2, backend_addr, running_backend, alice_core
):
    with freeze_time("2000-01-02"):
        await alice_core.fs.file_create("/foo.txt")

    with freeze_time("2000-01-03"):
        await alice_core.fs.file_write("/foo.txt", b"v1")

    async with wait_for_entries_synced(alice_core, ("/", "/foo.txt")):
        await alice_core.fs.sync("/foo.txt")

    async with core_factory(base_settings_path=tmpdir, backend_addr=backend_addr) as alice2_core2:

        async with wait_for_entries_synced(alice2_core2, ["/"]):
            await alice2_core2.login(alice2)

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

    async with wait_for_entries_synced(alice2_core2, ["/"]):
        await alice2_core2.login(alice2)

    for path in ("/", "/bar", "/foo.txt"):
        stat = await alice_core.fs.stat(path)
        stat2 = await alice2_core2.fs.stat(path)
        assert stat2 == stat


@pytest.mark.trio
async def test_fast_forward_on_offline_during_sync(
    autojump_clock, running_backend, alice_core, alice2, tcp_stream_spy
):

    # Connect core2 to backend on a special address to allow going offline
    # on this one while keeping core1 online
    core2_backend_addr = "tcp://core2.placeholder.com:9999"
    with tcp_stream_spy.install_hook(core2_backend_addr, running_backend.connection_factory):
        async with core_factory(
            backend_addr=core2_backend_addr, base_settings_path=alice_core.config.base_settings_path
        ) as alice2_core2:
            await alice2_core2.login(alice2)

            async with wait_for_entries_synced(alice_core, ["/"]):
                async with wait_for_entries_synced(alice2_core2, ("/", "/foo.txt")):

                    with freeze_time("2000-01-02"):
                        await alice2_core2.fs.file_create("/foo.txt")

                    with freeze_time("2000-01-03"):
                        await alice2_core2.fs.file_write("/foo.txt", b"v1")

                    await alice2_core2.fs.sync("/foo.txt")

            print("OFFLINE !!!")
            # core2 goes offline, other core is still connected to backend
            # with offline(core2_backend_addr):

            #     with freeze_time("2000-01-04"):
            #         await alice_core.fs.file_write("/foo.txt", b"v2")
            #         await alice_core.fs.folder_create("/bar")

            #     async withalice_core,  wait_for_entries_synced(("/", "/bar", "/foo.txt")):
            #         await alice_core.fs.sync("/")

            #     # Make sure we are really offline
            #     # with pytest.raises(SystemError):
            #     print('CORE 1 dump')
            #     for path in ("/", "/bar", "/foo.txt"):
            #         stat = await alice_core.fs.stat(path)
            #         print(stat)
            #     print('CORE 2 dump')
            #     for path in ("/", "/bar", "/foo.txt"):
            #         stat = await alice2_core2.fs.stat(path)
            #         print(stat)
            #     # x=  await alice2_core2.fs.stat("/foo.txt")
            #     # import pdb; pdb.set_trace()
            #     # print(x)

            # await wait_all_tasks_blocked(cushion=0.01)

            # for path in ("/", "/bar", "/foo.txt"):
            #     stat = await alice_core.fs.stat(path)
            #     stat2 = await alice2_core2.fs.stat(path)
            #     assert stat2 == stat
