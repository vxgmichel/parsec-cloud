import trio
import logbook
import json

from parsec.signals import get_signal
from parsec.schema import UnknownCheckedSchema, fields
from parsec.core.base import BaseAsyncComponent
from parsec.core.devices_manager import Device
from parsec.core.backend_connection import (
    backend_connection_factory,
    HandshakeError,
    BackendNotAvailable,
)


logger = logbook.Logger("parsec.core.backend_events_manager")


class BackendEventListenRepSchema(UnknownCheckedSchema):
    status = fields.CheckedConstant("ok", required=True)
    event = fields.String(required=True)
    subject = fields.String(missing=None)


backend_event_listen_rep_schema = BackendEventListenRepSchema()


class SubscribeBackendEventError(Exception):
    pass


class ListenBackendEventError(Exception):
    pass


class BackendEventsManager(BaseAsyncComponent):
    """
    Difference between signal and event:
    - signals are in-process notifications
    - events are message sent downstream from the backend to the client core
    """

    def __init__(self, device: Device, backend_addr: str):
        super().__init__()
        self.device = device
        self.backend_addr = backend_addr
        self._nursery = None
        self._subscribed_events = set()
        self._need_task_restart = trio.Event()
        self._el_task_info = None
        self._rm_task_info = None
        get_signal('backend.event.subscribe').connect(self._on_event_subscribe, weak=True)
        get_signal('backend.event.unsubscribe').connect(self._on_event_unsubscribe, weak=True)

    async def _on_event_subscribe(self, sender, event, subject=None):
        assert sender == 'local'
        self._subscribed_events.add((event, subject))
        self._need_task_restart.set()

    async def _on_event_unsubscribe(self, sender, event, subject=None):
        assert sender == 'local'
        try:
            self._subscribed_events.remove((event, subject))
        except KeyError:
            return
        self._need_task_restart.set()

    async def _init(self, nursery):
        self._nursery = nursery
        await self._restart_el_task()
        self._rm_task_info = await self._nursery.start(self._restart_manager_task)

    async def _teardown(self):
        await self._close_task()
        await self._close_rm_task()

    async def _restart_el_task(self):
        new_elt_info = await self._nursery.start(self._event_listener_task)
        if self._el_task_info:
            await self._close_task()
        self._el_task_info = new_elt_info

    async def _close_el_task(self):
        cancel_scope, closed_event = self._el_task_info
        cancel_scope.cancel()
        await closed_event.wait()
        self._el_task_info = None

    async def _close_rm_task(self):
        cancel_scope, closed_event = self._rm_task_info
        cancel_scope.cancel()
        await closed_event.wait()
        self._rm_task_info = None

    # async def subscribe_backend_event(self, event, subject=None):
    #     """
    #     Raises:
    #         KeyError: if event/subject couple has already been previously subscribed.
    #     """
    #     async with self._lock:
    #         key = (event, subject)
    #         if key in self._subscribed_events:
    #             raise KeyError("%s@%s already subscribed" % key)

    #         self._subscribed_events.add(key)
    #         logger.debug("Subscribe %s@%s, restarting event listener" % (event, subject))
    #         await self._teardown()
    #         await self._init(self._nursery)

    # async def unsubscribe_backend_event(self, event, subject=None):
    #     """
    #     Raises:
    #         KeyError: if event/subject couple has not been previously subscribed.
    #     """
    #     async with self._lock:
    #         self._subscribed_events.remove((event, subject))
    #         logger.debug("Unsubscribe %s@%s, restarting event listener" % (event, subject))
    #         await self._teardown()
    #         await self._init(self._nursery)

    async def _restart_manager_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        closed_event = trio.Event()
        try:
            with trio.open_cancel_scope() as cancel_scope:
                task_status.started((cancel_scope, closed_event))

                while True:
                    await self._need_task_restart.wait()
                    self._need_task_restart.clear()

        finally:
            closed_event.set()

    async def _event_listener_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        # Copy `self._subscribed_events` to avoid concurrent modifications
        subscribed_events = self._subscribed_events.copy()

        async def _event_pump(sock):
            # TODO: allow to subscribe to multiple events in a single query...
            for event, subject in subscribed_events:
                await sock.send({"cmd": "event_subscribe", "event": event, "subject": subject})
                rep = await sock.recv()
                if rep.get("status") != "ok":
                    raise SubscribeBackendEventError(
                        "Cannot subscribe to event `%s@%s`: %r" % (event, subject, rep)
                    )

            while True:
                await sock.send({"cmd": "event_listen"})
                rep = await sock.recv()
                _, errors = backend_event_listen_rep_schema.load(rep)
                if errors:
                    raise ListenBackendEventError(
                        "Bad reponse %r while listening for event: %r" % (rep, errors)
                    )

                subject = rep.get("subject")
                event = rep.get("event")
                if subject is None:
                    get_signal(event).send()
                else:
                    get_signal(event).send(subject)

        try:
            closed_event = trio.Event()
            with trio.open_cancel_scope() as cancel_scope:
                task_status.started((cancel_scope, closed_event))

                while True:
                    try:
                        sock = await backend_connection_factory(self.backend_addr, self.device)
                        get_signal('backend.online').send('local')
                        await _event_pump(sock)
                    except (
                        BackendNotAvailable,
                        trio.BrokenStreamError,
                        trio.ClosedStreamError,
                    ) as exc:
                        # In case of connection failure, wait a bit and restart
                        get_signal('backend.offline').send('local')
                        logger.debug(
                            "Connection lost with backend ({}), restarting connection...", exc
                        )
                        await trio.sleep(1)
                    except (
                        SubscribeBackendEventError,
                        ListenBackendEventError,
                        json.JSONDecodeError,
                    ):
                        logger.exception(
                            "Invalid response sent by backend, restarting connection..."
                        )
                        await trio.sleep(1)
                    except HandshakeError as exc:
                        # Handshake error means there is no need retrying the connection
                        # Only thing we can do is sending a signal to notify the
                        # trouble...
                        # TODO: think about this kind of signal format
                        get_signal("panic").send(exc)
        finally:
            closed_event.set()
