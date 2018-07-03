import trio
from trio.hazmat import current_clock

from parsec.signals import get_signal
from parsec.core.backend_connection import BackendNotAvailable


MIN_WAIT = 1
MAX_WAIT = 60


def timestamp():
    # Use time from trio clock to easily mock it
    return current_clock().current_time()


class SyncMonitor:
    def __init__(self, local_manifest_fs, syncer):
        self._local_manifest_fs = local_manifest_fs
        self._syncer = syncer
        self._task_cancel_scope = None
        self._updated_entries = {}
        self._new_event = trio.Event()

    async def init(self, nursery):
        self._task_cancel_scope = await nursery.start(self._task)

    async def teardown(self):
        self._task_cancel_scope.cancel()

    async def _task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        backend_online_event = trio.Event()

        def _on_backend_online(*args):
            backend_online_event.set()

        get_signal("backend.online").connect(_on_backend_online, weak=True)

        def _on_backend_offline(*args):
            backend_online_event.clear()
            event_listener_scope.cancel()

        get_signal("backend.offline").connect(_on_backend_offline, weak=True)

        async with trio.open_nursery() as nursery:
            task_status.started(nursery.cancel_scope)
            while True:
                try:
                    with trio.open_cancel_scope() as event_listener_scope:
                        await self._syncer.full_sync()
                        await self._listen_sync_loop()

                except BackendNotAvailable:
                    await backend_online_event.wait()

    async def _listen_sync_loop(self):
        updated_entries = {}
        new_event = trio.Event()

        def _on_entry_updated(sender, id):
            try:
                first_updated, _ = updated_entries[id]
                last_updated = timestamp()
            except KeyError:
                first_updated = last_updated = timestamp()
            print(id, "updated")
            updated_entries[id] = (first_updated, last_updated)
            new_event.set()

        get_signal("fs.entry.updated").connect(_on_entry_updated, weak=True)

        async with trio.open_nursery() as nursery:
            while True:
                await new_event.wait()
                new_event.clear()
                await self._listen_sync_step(updated_entries)

                if updated_entries:

                    async def _wait():
                        await trio.sleep(MIN_WAIT)
                        new_event.set()

                    nursery.start_soon(_wait)

    async def _listen_sync_step(self, updated_entries):
        now = timestamp()
        print("WAKE UP MR. FREEMAN !", updated_entries.keys())

        for id, (first_updated, last_updated) in updated_entries.items():
            if now - first_updated > MAX_WAIT:
                await self._syncer.sync_by_id(id)
                break

        else:
            for id, (_, last_updated) in updated_entries.items():
                if now - last_updated > MIN_WAIT:
                    await self._syncer.sync_by_id(id)
                    break

            else:
                id = None

        if id:
            _, new_last_updated = updated_entries[id]
            # This entry has been modified again during the sync
            if new_last_updated != last_updated:
                updated_entries[id] = (last_updated, new_last_updated)
            else:
                del updated_entries[id]
