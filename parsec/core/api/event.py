import trio
import logbook

from parsec.core.app import Core, ClientContext
from parsec.core.backend_connection import BackendNotAvailable
from parsec.schema import UnknownCheckedSchema, BaseCmdSchema, fields, validate


logger = logbook.Logger("parsec.api.event")

ALLOWED_SIGNALS = {"ping", "fuse_mountpoint_need_stop", "new_sharing"}
ALLOWED_BACKEND_EVENTS = {"device_try_claim_submitted"}


class BackendGetConfigurationTrySchema(UnknownCheckedSchema):
    status = fields.CheckedConstant("ok", required=True)
    device_name = fields.String(required=True)
    configuration_status = fields.String(required=True)
    device_verify_key = fields.Base64Bytes(required=True)
    exchange_cipherkey = fields.Base64Bytes(required=True)


backend_get_configuration_try_schema = BackendGetConfigurationTrySchema()


class cmd_EVENT_LISTEN_Schema(BaseCmdSchema):
    wait = fields.Boolean(missing=True)


class cmd_EVENT_SUBSCRIBE_Schema(BaseCmdSchema):
    event = fields.String(
        required=True, validate=validate.OneOf(ALLOWED_SIGNALS | ALLOWED_BACKEND_EVENTS)
    )
    subject = fields.String(missing=None)


async def event_subscribe(req: dict, client_ctx: ClientContext, core: Core) -> dict:
    if not core.auth_device:
        return {"status": "login_required", "reason": "Login required"}

    msg = cmd_EVENT_SUBSCRIBE_Schema().load(req)
    event = msg["event"]
    subject = msg["subject"]

    try:
        client_ctx.subscribe_signal(event, subject)
    except KeyError as exc:
        return {
            "status": "already_subscribed",
            "reason": "Already subscribed to this event/subject couple",
        }

    if event in ALLOWED_BACKEND_EVENTS:
        core.signal_ns.signal("backend.event.subscribe").send(
            core.auth_device.id, event=event, subject=subject
        )

        backend_event_manager_restarted = trio.Event()

        key = (event, subject)

        def _on_backend_event_subscribed(sender, events):
            if key in events:
                backend_event_manager_restarted.set()

        with core.signal_ns.signal("backend.event.subscribed").temporarily_connected_to(
            _on_backend_event_subscribed
        ):
            await backend_event_manager_restarted.wait()

    return {"status": "ok"}


async def event_unsubscribe(req: dict, client_ctx: ClientContext, core: Core) -> dict:
    if not core.auth_device:
        return {"status": "login_required", "reason": "Login required"}

    msg = cmd_EVENT_SUBSCRIBE_Schema().load(req)
    event = msg["event"]
    subject = msg["subject"]

    try:
        # Note here we consider `None` as `blinker.ANY` for simplicity sake
        if subject:
            client_ctx.unsubscribe_signal(event, subject)
        else:
            client_ctx.unsubscribe_signal(event)
    except KeyError as exc:
        return {"status": "not_subscribed", "reason": "Not subscribed to this event/subject couple"}

    # Cannot unsubscribe the backend event given it could be in use by another
    # client context...

    return {"status": "ok"}


async def event_listen(req: dict, client_ctx: ClientContext, core: Core) -> dict:
    if not core.auth_device:
        return {"status": "login_required", "reason": "Login required"}

    msg = cmd_EVENT_LISTEN_Schema().load(req)
    if msg["wait"]:
        event, subject, kwargs = await client_ctx.received_signals.get()
    else:
        try:
            event, subject, kwargs = client_ctx.received_signals.get_nowait()
        except trio.WouldBlock:
            return {"status": "ok"}

    # TODO: make more generic
    if event == "device_try_claim_submitted":
        config_try_id = kwargs["config_try_id"]
        try:
            rep = await core.backend_connection.send(
                {"cmd": "device_get_configuration_try", "config_try_id": config_try_id}
            )
        except BackendNotAvailable:
            return {"status": "backend_not_availabled", "reason": "Backend not available"}

        _, errors = backend_get_configuration_try_schema.load(rep)
        if errors:
            return {
                "status": "backend_error",
                "reason": "Bad response from backend: %r (%r)" % (rep, errors),
            }

        return {"status": "ok", **kwargs}

    else:
        return {"status": "ok", "event": event, "subject": subject, **kwargs}


async def event_list_subscribed(req: dict, client_ctx: ClientContext, core: Core) -> dict:
    if not core.auth_device:
        return {"status": "login_required", "reason": "Login required"}

    BaseCmdSchema().load(req)  # empty msg expected
    return {"status": "ok", "subscribed": list(client_ctx.registered_signals.keys())}
