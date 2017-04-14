import json
import asyncio
import random
import string
from marshmallow import fields
from marshmallow.validate import OneOf
from logbook import Logger

from parsec.exceptions import ParsecError, HandshakeError
from parsec.session import anonymous_handshake
from parsec.tools import BaseCmdSchema


class BaseClientContext:

    async def recv(self):
        raise NotImplementedError()

    async def send(self, body):
        raise NotImplementedError()


def _unique_enough_id():
    # Colision risk is high, but this is pretty fine (and much more readable
    # than a uuid4) for giving id to connections
    return ''.join([random.choice(string.ascii_letters + string.digits) for ch in range(8)])


class BaseServer:
    def __init__(self, handshake=anonymous_handshake):
        self._cmds = {
            'list_cmds': self.__cmd_LIST_CMDS,
            'subscribe': self.__cmd_SUBSCRIBE
        }
        self._events = {}
        self._services = {}
        self._handshake = handshake

    async def __cmd_LIST_CMDS(self, session, msg):
        return {'status': 'ok', 'cmds': list(self._cmds.keys())}

    async def __cmd_SUBSCRIBE(self, session, msg):

        class cmd_SUBSCRIBE_Schema(BaseCmdSchema):
            event = fields.String(required=True, validate=OneOf(self._events.keys()))
            sender = fields.String(required=True)

        msg = cmd_SUBSCRIBE_Schema().load(msg)

        event = msg['event']

        def on_event(sender):
            session.received_events.put_nowait((event, sender))

        # Attach the callback to the session to make them have the same
        # lifetime given event registration expires when callback is destroyed
        setattr(session, '_cb_%s' % event, on_event)
        self._events[event].connect(on_event, sender=msg['sender'])
        return {'status': 'ok'}

    def register_service(self, service):
        self._services[service.name] = service
        for cmd_name, cb in service.cmds.items():
            self.register_cmd(cmd_name, cb)
        for event_name, event in service.events.items():
            self.register_event(event_name, event)

    def register_cmd(self, name, cb):
        if name in self._cmds:
            raise RuntimeError('Command `%s` already registered.' % name)
        self._cmds[name] = cb

    def register_event(self, name, event):
        if name in self._events:
            raise RuntimeError('Event `%s` already registered.' % name)
        self._events[name] = event

    @staticmethod
    def _load_raw_cmd(raw):
        if not raw:
            return None
        try:
            msg = json.loads(raw.decode())
            if isinstance(msg.get('cmd'), str):
                return msg
            else:
                return None
        except json.decoder.JSONDecodeError:
            pass
        # Not a JSON payload, try cmdline mode
        splitted = raw.decode().strip().split(' ')
        cmd = splitted[0]
        raw_msg = '{"cmd": "%s"' % cmd
        for data in splitted[1:]:
            if '=' not in data:
                return None
            raw_msg += ', "%s": %s' % tuple(data.split('=', maxsplit=1))
        raw_msg += '}'
        try:
            return json.loads(raw_msg)
        except json.decoder.JSONDecodeError:
            pass
        # Nothing worked :'-(
        return None

    async def on_connection(self, context: BaseClientContext):
        conn_log = Logger('Connection ' + _unique_enough_id())
        conn_log.debug('Connection started')
        # Handle handshake if auth is required
        try:
            session = await self._handshake(context)
        except HandshakeError as exc:
            await context.send(exc.to_raw())
            return
        get_event = asyncio.ensure_future(session.received_events.get())
        get_cmd = asyncio.ensure_future(context.recv())
        while True:
            # Wait for two things:
            # - User's command
            # - Event subscribed by user
            # Note user's command should have been replied before sending an event notification
            done, pending = await asyncio.wait((get_event, get_cmd), return_when='FIRST_COMPLETED')
            if get_event in done:
                event, sender = get_event.result()
                conn_log.debug('Got event: %s@%s' % (event, sender))
                resp = {'event': event, 'sender': sender}
                await context.send(json.dumps(resp).encode())
                # Restart watch on incoming notifications
                get_event = asyncio.ensure_future(session.received_events.get())
            else:
                raw_cmd = get_cmd.result()
                if not raw_cmd:
                    get_event.cancel()
                    conn_log.debug('Connection stopped')
                    return
                conn_log.debug('Received: %r' % raw_cmd)
                msg = self._load_raw_cmd(raw_cmd)
                if msg is None:
                    resp = {"status": "bad_message", "label": "Message is not a valid JSON."}
                else:
                    cmd = self._cmds.get(msg['cmd'])
                    if not cmd:
                        resp = {"status": "badcmd", "label": "Unknown command `%s`" % msg['cmd']}
                    else:
                        try:
                            resp = await cmd(session, msg)
                        except ParsecError as exc:
                            resp = exc.to_dict()
                conn_log.debug('Replied: %r' % resp)
                await context.send(json.dumps(resp).encode())
                # Restart watch on incoming messages
                get_cmd = asyncio.ensure_future(context.recv())

    async def bootstrap_services(self):
        errors = []
        for service in self._services.values():
            try:
                boot = service.inject_services()
                dep = next(boot)
                while True:
                    if dep not in self._services:
                        errors.append("Service `%s` required unknown service `%s`" %
                            (service.name, dep))
                        break
                    dep = boot.send(self._services[dep])
            except StopIteration:
                pass
        if errors:
            raise RuntimeError(errors)
        for service in self._services.values():
            await service.bootstrap()

    async def teardown_services(self):
        for service in self._services.values():
            await service.teardown()

    def start(self):
        raise NotImplementedError()