import trio
import attr
from typing import Optional
from pathlib import Path
from async_generator import asynccontextmanager

from parsec.event_bus import EventBus
from parsec.core.types import LocalDevice
from parsec.core.config import CoreConfig
from parsec.core.backend_connection import (
    backend_cmds_factory,
    backend_listen_events,
    monitor_backend_connection,
)
from parsec.core.encryption_manager import EncryptionManager
from parsec.core.mountpoint import mountpoint_manager_factory
from parsec.core.beacons_monitor import monitor_beacons
from parsec.core.messages_monitor import monitor_messages
from parsec.core.sync_monitor import monitor_sync
from parsec.core.fs import FS
from parsec.core.local_db import LocalDB


@attr.s(frozen=True, slots=True)
class LoggedCore:
    config = attr.ib()
    device = attr.ib()
    local_db = attr.ib()
    event_bus = attr.ib()
    encryption_manager = attr.ib()
    mountpoint = attr.ib()
    backend_cmds = attr.ib()
    fs = attr.ib()


@asynccontextmanager
async def logged_core_factory(
    config: CoreConfig,
    device: LocalDevice,
    event_bus: Optional[EventBus] = None,
    mountpoint: Optional[Path] = None,
):
    event_bus = event_bus or EventBus()

    # Plenty of nested scope to order components init/teardown
    async with trio.open_nursery() as root_nursery:
        # TODO: Currently backend_listen_events connect to backend and
        # switch to listen events mode, then monitors kick in and send it
        # events about which beacons to listen on, obliging to restart the
        # listen connection...
        backend_online = await root_nursery.start(backend_listen_events, device, event_bus)

        async with backend_cmds_factory(
            device.backend_addr,
            device.device_id,
            device.signing_key,
            config.backend_max_connections,
        ) as backend_cmds_pool:

            local_db = LocalDB(config.data_base_dir / device.device_id)

            encryption_manager = EncryptionManager(device, local_db, backend_cmds_pool)
            fs = FS(device, local_db, backend_cmds_pool, encryption_manager, event_bus)

            async with trio.open_nursery() as monitor_nursery:
                # Finally start monitors

                # Monitor connection must be first given it will watch on
                # other monitors' events
                await monitor_nursery.start(monitor_backend_connection, backend_online, event_bus)
                await monitor_nursery.start(monitor_beacons, device, fs, event_bus)
                await monitor_nursery.start(monitor_messages, backend_online, fs, event_bus)
                await monitor_nursery.start(monitor_sync, backend_online, fs, event_bus)

                if config.mountpoint_enabled:
                    # TODO: rework mountpoint manager to avoid init/teardown
                    mountpoint_manager = mountpoint_manager_factory(fs, event_bus)
                    await mountpoint_manager.init(monitor_nursery)
                    if not mountpoint:
                        mountpoint = config.mountpoint_base_dir / device.device_id
                    await mountpoint_manager.start(mountpoint)

                else:
                    mountpoint = None

                try:
                    yield LoggedCore(
                        config=config,
                        device=device,
                        local_db=local_db,
                        event_bus=event_bus,
                        encryption_manager=encryption_manager,
                        mountpoint=mountpoint,
                        backend_cmds=backend_cmds_pool,
                        fs=fs,
                    )
                    root_nursery.cancel_scope.cancel()

                finally:
                    if config.mountpoint_enabled:
                        await mountpoint_manager.teardown()
