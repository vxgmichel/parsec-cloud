import asyncio
import blinker
import inspect
import json
import websockets

from parsec.backend import (
    MockedGroupService, InMemoryMessageService, MockedVlobService, MockedNamedVlobService
)
from parsec.core.cache import cache
from parsec.exceptions import exception_from_status
from parsec.service import BaseService
from parsec.tools import logger


def _patch_service_event_namespace(service, ns):
    """
    By default, service's events uses blinker's global namespace.
    This function patch the given service to use another events namespace.
    """
    for k in dir(service):
        v = getattr(service, k)
        if isinstance(v, blinker.Signal):
            setattr(service, k, ns.signal(v.name))
    service._events = {k: ns.signal(v.name) for k, v in service._events.items()}


def cached(method):

    def get_arg(arg, args, kwargs):
        try:
            return kwargs[arg]
        except KeyError:
            properties = inspect.getargspec(method)
            arg_index = properties.args.index(arg)
            try:
                return args[arg_index]
            except IndexError:
                defaults_values = dict(zip(reversed(properties.args),
                                           reversed(properties.defaults)))
                return defaults_values[arg]

    async def inner(*args, **kwargs):
        method_name = method.__name__
        response = None
        if method_name in ['named_vlob_create', 'vlob_create']:
            blob = get_arg('blob', args, kwargs)
            response = await method(*args, **kwargs)
            cached_content = {'status': 'ok', 'id': response['id'], 'blob': blob, 'version': 1}
            await cache.set(response['id'] + ':1', cached_content)
        elif method_name in ['named_vlob_read', 'vlob_read']:
            id = get_arg('id', args, kwargs)
            version = get_arg('version', args, kwargs)
            if version:
                response = await cache.get(id + ':' + str(version))
            if not response:
                response = await method(*args, **kwargs)
                await cache.set(id + ':' + str(response['version']), response)
        elif method_name in ['named_vlob_update', 'vlob_update']:
            id = get_arg('id', args, kwargs)
            blob = get_arg('blob', args, kwargs)
            version = get_arg('version', args, kwargs)
            response = await method(*args, **kwargs)
            cached_content = {'status': 'ok', 'id': id, 'blob': blob, 'version': version}
            await cache.set(id + ':' + str(version), cached_content)
        else:
            response = await method(*args, **kwargs)
        return response

    return inner


class BaseBackendAPIService(BaseService):

    name = 'BackendAPIService'


class BackendAPIService(BaseBackendAPIService):

    def __init__(self, backend_url, watchdog=None):
        super().__init__()
        self._resp_queue = asyncio.Queue()
        assert backend_url.startswith('ws://') or backend_url.startswith('wss://')
        self._backend_url = backend_url
        self._websocket = None
        self._ws_recv_handler_task = None
        self._watchdog_task = None
        self._watchdog_time = watchdog

    async def connect_event(self, event, sender, cb):
        assert event in ('on_vlob_updated', 'on_named_vlob_updated', 'on_message_arrived')
        msg = {'cmd': 'subscribe', 'event': event, 'sender': sender}
        await self._send_cmd(msg)
        blinker.signal(event).connect(cb, sender=sender)

    async def bootstrap(self):
        assert not self._websocket, "Service already bootstrapped"
        self._websocket = await websockets.connect(self._backend_url)
        self._ws_recv_handler_task = asyncio.ensure_future(self._ws_recv_handler())
        if self._watchdog_time:
            self._watchdog_task = asyncio.ensure_future(self._watchdog())
        await super().bootstrap()

    async def teardown(self):
        assert self._websocket, "Service hasn't been bootstrapped"
        for task in (self._ws_recv_handler_task, self._watchdog_task):
            if not task:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self._websocket.close()

    async def _watchdog(self):
        while True:
            await asyncio.sleep(self._watchdog_time)
            logger.debug('Watchdog ping to backend.')
            await self._websocket.ping()

    async def _ws_recv_handler(self):
        # Given command responses and notifications are all send through the
        # same websocket, separate them here, passing command response thanks
        # to a Queue.
        while True:
            raw = await self._websocket.recv()
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                recv = json.loads(raw)
                if 'status' in recv:
                    # Message response
                    self._resp_queue.put_nowait(recv)
                else:
                    # Event
                    blinker.signal(recv['event']).send(recv['sender'])
            except (KeyError, TypeError, json.JSONDecodeError):
                # Dummy ???
                logger.warning('Backend server sent invalid message: %s' % recv)

    async def _send_cmd(self, msg):
        await self._websocket.send(json.dumps(msg))
        ret = await self._resp_queue.get()
        status = ret['status']
        if status == 'ok':
            return ret
        else:
            raise exception_from_status(status)(ret['label'])

    async def group_create(self, name):
        msg = {'cmd': 'group_create', 'name': name}
        return await self._send_cmd(msg)

    async def group_read(self, name):
        msg = {'cmd': 'group_read', 'name': name}
        return await self._send_cmd(msg)

    async def group_add_identities(self, name, identities, admin=False):
        msg = {'cmd': 'group_add_identities',
               'name': name,
               'identities': identities,
               'admin': admin}
        return await self._send_cmd(msg)

    async def group_remove_identities(self, name, identities, admin=False):
        msg = {'cmd': 'group_remove_identities',
               'name': name,
               'identities': identities,
               'admin': admin}
        return await self._send_cmd(msg)

    async def message_new(self, recipient, body):
        msg = {'cmd': 'message_new', 'recipient': recipient, 'body': body}
        return await self._send_cmd(msg)

    async def message_get(self, recipient, offset=0):
        msg = {'cmd': 'message_get', 'recipient': recipient, 'offset': offset}
        ret = await self._send_cmd(msg)
        return [msg['body'] for msg in ret['messages']]

    @cached
    async def named_vlob_create(self, id, blob=''):
        assert isinstance(blob, str)
        msg = {'cmd': 'named_vlob_create', 'id': id, 'blob': blob}
        return await self._send_cmd(msg)

    @cached
    async def named_vlob_read(self, id, trust_seed, version=None):
        assert isinstance(id, str)
        msg = {'cmd': 'named_vlob_read', 'id': id, 'trust_seed': trust_seed}
        if version:
            msg['version'] = version
        return await self._send_cmd(msg)

    @cached
    async def named_vlob_update(self, id, version, trust_seed, blob=''):
        msg = {
            'cmd': 'named_vlob_update',
            'id': id,
            'version': version,
            'trust_seed': trust_seed,
            'blob': blob
        }
        return await self._send_cmd(msg)

    @cached
    async def vlob_create(self, blob=''):
        assert isinstance(blob, str)
        msg = {'cmd': 'vlob_create', 'blob': blob}
        return await self._send_cmd(msg)

    @cached
    async def vlob_read(self, id, trust_seed, version=None):
        assert isinstance(id, str)
        msg = {'cmd': 'vlob_read', 'id': id, 'trust_seed': trust_seed}
        if version:
            msg['version'] = version
        return await self._send_cmd(msg)

    @cached
    async def vlob_update(self, id, version, trust_seed, blob=''):
        msg = {
            'cmd': 'vlob_update',
            'id': id,
            'version': version,
            'trust_seed': trust_seed,
            'blob': blob
        }
        return await self._send_cmd(msg)


class MockedBackendAPIService(BaseBackendAPIService):

    def __init__(self):
        super().__init__()
        self._group_service = MockedGroupService()
        self._message_service = InMemoryMessageService()
        self._named_vlob_service = MockedNamedVlobService()
        self._vlob_service = MockedVlobService()
        # Backend services should not share the same event namespace than
        # core ones
        self._backend_event_ns = blinker.Namespace()
        _patch_service_event_namespace(self._group_service, self._backend_event_ns)
        _patch_service_event_namespace(self._message_service, self._backend_event_ns)
        _patch_service_event_namespace(self._named_vlob_service, self._backend_event_ns)
        _patch_service_event_namespace(self._vlob_service, self._backend_event_ns)

    async def connect_event(self, event, sender, cb):
        assert event in ('on_vlob_updated', 'on_named_vlob_updated', 'on_message_arrived')
        self._backend_event_ns.signal(event).connect(cb, sender=sender)

    async def group_create(self, name):
        msg = {'cmd': 'group_create', 'name': name}
        return await self._group_service._cmd_CREATE(None, msg)

    async def group_read(self, name):
        msg = {'cmd': 'group_read', 'name': name}
        return await self._group_service._cmd_READ(None, msg)

    async def group_add_identities(self, name, identities, admin=False):
        msg = {'cmd': 'group_add_identities',
               'name': name,
               'identities': identities,
               'admin': admin}
        return await self._group_service._cmd_ADD_IDENTITIES(None, msg)

    async def group_remove_identities(self, name, identities, admin=False):
        msg = {'cmd': 'group_remove_identities',
               'name': name,
               'identities': identities,
               'admin': admin}
        return await self._group_service._cmd_REMOVE_IDENTITIES(None, msg)

    async def message_new(self, recipient, body):
        msg = {'cmd': 'message_new', 'recipient': recipient, 'body': body}
        return await self._message_service._cmd_NEW(None, msg)

    async def message_get(self, recipient, offset=0):
        msg = {'cmd': 'message_get', 'recipient': recipient, 'offset': offset}
        ret = await self._message_service._cmd_GET(None, msg)
        return [msg['body'] for msg in ret['messages']]

    @cached
    async def named_vlob_create(self, id, blob=''):
        assert isinstance(blob, str)
        msg = {'cmd': 'named_vlob_create', 'id': id, 'blob': blob}
        return await self._named_vlob_service._cmd_CREATE(None, msg)

    @cached
    async def named_vlob_read(self, id, trust_seed, version=None):
        msg = {'cmd': 'named_vlob_read', 'id': id, 'trust_seed': trust_seed}
        if version:
            msg['version'] = version
        return await self._named_vlob_service._cmd_READ(None, msg)

    @cached
    async def named_vlob_update(self, id, version, trust_seed, blob=''):
        msg = {
            'cmd': 'named_vlob_update',
            'id': id,
            'version': version,
            'trust_seed': trust_seed,
            'blob': blob
        }
        await self._named_vlob_service._cmd_UPDATE(None, msg)

    @cached
    async def vlob_create(self, blob=''):
        assert isinstance(blob, str)
        msg = {'cmd': 'vlob_create', 'blob': blob}
        return await self._vlob_service._cmd_CREATE(None, msg)

    @cached
    async def vlob_read(self, id, trust_seed, version=None):
        msg = {'cmd': 'vlob_read', 'id': id, 'trust_seed': trust_seed}
        if version:
            msg['version'] = version
        return await self._vlob_service._cmd_READ(None, msg)

    @cached
    async def vlob_update(self, id, version, trust_seed, blob=''):
        msg = {
            'cmd': 'vlob_update',
            'id': id,
            'version': version,
            'trust_seed': trust_seed,
            'blob': blob
        }
        await self._vlob_service._cmd_UPDATE(None, msg)
