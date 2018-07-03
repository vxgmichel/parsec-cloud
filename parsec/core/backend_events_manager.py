import trio
import logbook
import json

from parsec.signals import get_signal
from parsec.schema import UnknownCheckedSchema, OneOfSchema, fields
from parsec.core.base import BaseAsyncComponent
from parsec.core.devices_manager import Device
from parsec.core.backend_connection import (
    backend_connection_factory,
    HandshakeError,
    BackendNotAvailable,
)


logger = logbook.Logger("parsec.core.backend_events_manager")


class BackendEventPingRepSchema(UnknownCheckedSchema):
    status = fields.CheckedConstant("ok", required=True)
    sender = fields.String(required=True)
    event = fields.String(required=True)
    msg = fields.String(missing=None)


class BackendEventListenRepSchema(OneOfSchema):
    type_field = "event"
    type_field_remove = False
    type_schemas = {"ping": BackendEventPingRepSchema()}

    def get_obj_type(self, obj):
        return obj["event"]


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
        self._backend_offline = True
        self._subscribed_events = set()
        self._subscribed_events_changed = trio.Event()
        self._task_info = None
        get_signal("backend.event.subscribe").connect(self._on_event_subscribe, weak=True)
        get_signal("backend.event.unsubscribe").connect(self._on_event_unsubscribe, weak=True)

    def _on_event_subscribe(self, sender, event, subject=None):
        assert sender == self.device.id
        key = (event, subject)
        if key in self._subscribed_events:
            return
        self._subscribed_events.add(key)
        self._subscribed_events_changed.set()

    def _on_event_unsubscribe(self, sender, event, subject=None):
        assert sender == self.device.id
        try:
            self._subscribed_events.remove((event, subject))
        except KeyError:
            return
        self._subscribed_events_changed.set()

    async def _init(self, nursery):
        self._nursery = nursery
        self._task_info = await self._nursery.start(self._task)

    async def _teardown(self):
        cancel_scope, closed_event = self._task_info
        cancel_scope.cancel()
        await closed_event.wait()
        self._task_info = None

    async def _restart_el_task(self):
        new_elt_info = await self._nursery.start(self._event_listener_task)
        if self._el_task_info:
            await self._close_task()
        self._el_task_info = new_elt_info

    def _event_pump_lost(self):
        print("LOST", self._backend_offline)
        self._backend_offline = True
        get_signal("backend.offline").send(self.device.id)

    def _event_pump_ready(self):
        print("READY", self._backend_offline)
        if self._backend_offline:
            self._backend_offline = False
            get_signal("backend.online").send(self.device.id)
        get_signal("backend.event.subscribed").send(self.device.id, events=self._subscribed_events)

    async def _task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        closed_event = trio.Event()
        try:
            with trio.open_cancel_scope() as cancel_scope:
                task_status.started((cancel_scope, closed_event))

                await self._event_listener_manager()
        finally:
            closed_event.set()

    async def _event_listener_manager(self):
        while True:
            try:

                async with trio.open_nursery() as nursery:
                    self._subscribed_events_changed.clear()
                    event_pump_cancel_scope = await nursery.start(self._event_pump)
                    self._event_pump_ready()
                    while True:
                        await self._subscribed_events_changed.wait()
                        self._subscribed_events_changed.clear()
                        new_cancel_scope = await nursery.start(self._event_pump)
                        self._event_pump_ready()
                        event_pump_cancel_scope.cancel()
                        event_pump_cancel_scope = new_cancel_scope

            except (BackendNotAvailable, trio.BrokenStreamError, trio.ClosedStreamError) as exc:
                # In case of connection failure, wait a bit and restart
                self._event_pump_lost()
                logger.debug("Connection lost with backend ({}), restarting connection...", exc)
                # TODO: add backoff factor
                await trio.sleep(1)

            except (SubscribeBackendEventError, ListenBackendEventError, json.JSONDecodeError):
                self._event_pump_lost()
                logger.exception("Invalid response sent by backend, restarting connection...")
                # TODO: remove the event that provoked the error ?
                await trio.sleep(1)

            except HandshakeError as exc:
                # Handshake error means there is no need retrying the connection
                # Only thing we can do is sending a signal to notify the
                # trouble...
                # TODO: think about this kind of signal format
                self._event_pump_lost()
                get_signal("panic").send(exc)

    async def _event_pump(self, *, task_status=trio.TASK_STATUS_IGNORED):
        with trio.open_cancel_scope() as cancel_scope:
            sock = await backend_connection_factory(self.backend_addr, self.device)

            # Copy `self._subscribed_events` to avoid concurrent modifications
            subscribed_events = self._subscribed_events.copy()

            # TODO: allow to subscribe to multiple events in a single query...
            for event, subject in subscribed_events:
                await sock.send({"cmd": "event_subscribe", "event": event, "subject": subject})
                rep = await sock.recv()
                if rep.get("status") != "ok":
                    raise SubscribeBackendEventError(
                        "Cannot subscribe to event `%s@%s`: %r" % (event, subject, rep)
                    )

            task_status.started(cancel_scope)
            while True:
                await sock.send({"cmd": "event_listen"})
                rep = await sock.recv()
                _, errors = backend_event_listen_rep_schema.load(rep)
                if errors:
                    raise ListenBackendEventError(
                        "Bad reponse %r while listening for event: %r" % (rep, errors)
                    )

                rep.pop("status")
                sender = rep.pop("sender")
                get_signal(rep["event"]).send(sender, **rep)
