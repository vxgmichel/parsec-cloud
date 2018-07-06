import trio
import pendulum
from unittest.mock import Mock
from inspect import iscoroutinefunction
from async_generator import asynccontextmanager
from copy import deepcopy
from nacl.public import PrivateKey
from nacl.signing import SigningKey

from parsec.core import Core, CoreConfig
from parsec.core.fs.data import new_access, new_workspace_manifest, remote_to_local_manifest
from parsec.core.devices_manager import Device
from parsec.core.local_db import LocalDB, LocalDBMissingEntry
from parsec.handshake import ClientHandshake, AnonymousClientHandshake
from parsec.networking import CookedSocket
from parsec.backend import BackendApp, BackendConfig


class InMemoryLocalDB(LocalDB):
    def __init__(self):
        self._data = {}

    def get(self, access):
        try:
            return deepcopy(self._data[access["id"]])
        except KeyError:
            raise LocalDBMissingEntry(access)

    def set(self, access, manifest):
        self._data[access["id"]] = deepcopy(manifest)

    def clear(self, access):
        del self._data[access["id"]]


def freeze_time(timestr):
    return pendulum.test(pendulum.parse(timestr))


class AsyncMock(Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        spec = kwargs.get("spec")
        if spec:
            for field in dir(spec):
                if iscoroutinefunction(getattr(spec, field)):
                    getattr(self, field).is_async = True

    async def __async_call(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if getattr(self, "is_async", False) is True:
            if iscoroutinefunction(self.side_effect):
                return self.side_effect(*args, **kwargs)

            else:
                return self.__async_call(*args, **kwargs)

        else:
            return super().__call__(*args, **kwargs)


class FreezeTestOnBrokenStreamCookedSocket(CookedSocket):
    """
    When a server crashes during test, it is possible the client coroutine
    receives a `trio.BrokenStreamError` exception. Hence we end up with two
    exceptions: the server crash (i.e. the original exception we are interested
    into) and the client not receiving an answer.
    The solution is simply to freeze the coroutine receiving the broken stream
    error until it will be cancelled by the original exception bubbling up.
    """

    async def send(self, msg):
        try:
            return await super().send(msg)

        except trio.BrokenStreamError as exc:
            # Wait here until this coroutine is cancelled
            await trio.sleep_forever()

    async def recv(self):
        try:
            return await super().recv()

        except trio.BrokenStreamError as exc:
            # Wait here until this coroutine is cancelled
            await trio.sleep_forever()


# TODO: Rename to serve_app ?
@asynccontextmanager
async def run_app(app):
    async with trio.open_nursery() as nursery:

        async def connection_factory(*args, **kwargs):
            right, left = trio.testing.memory_stream_pair()
            nursery.start_soon(app.handle_client, left)
            return right

        try:
            yield connection_factory

        finally:
            nursery.cancel_scope.cancel()


@asynccontextmanager
async def backend_factory(**config):
    config = BackendConfig(**config)
    backend = BackendApp(config)
    async with trio.open_nursery() as nursery:
        await backend.init(nursery)
        try:
            yield backend

        finally:
            await backend.teardown()
            nursery.cancel_scope.cancel()


@asynccontextmanager
async def connect_backend(backend, auth_as=None):
    async with run_app(backend) as connection_factory:
        sockstream = await connection_factory()
        sock = FreezeTestOnBrokenStreamCookedSocket(sockstream)
        if auth_as:
            # Handshake
            if auth_as == "anonymous":
                ch = AnonymousClientHandshake()
            else:
                ch = ClientHandshake(auth_as.id, auth_as.device_signkey)
            challenge_req = await sock.recv()
            answer_req = ch.process_challenge_req(challenge_req)
            await sock.send(answer_req)
            result_req = await sock.recv()
            ch.process_result_req(result_req)

        yield sock


@asynccontextmanager
async def connect_core(core):
    async with run_app(core) as connection_factory:
        sockstream = await connection_factory()
        sock = FreezeTestOnBrokenStreamCookedSocket(sockstream)

        yield sock


@asynccontextmanager
async def core_factory(**config):
    config = CoreConfig(**config)
    core = Core(config)
    async with trio.open_nursery() as nursery:
        await core.init(nursery)
        try:
            yield core

        finally:
            await core.teardown()
            nursery.cancel_scope.cancel()


def bootstrap_device(user_id, device_name):
    return bootstrap_devices(user_id, (device_name,))[0]


def bootstrap_devices(user_id, devices_names):
    user_privkey = PrivateKey.generate().encode()
    first_device_id = "%s@%s" % (user_id, devices_names[0])

    with freeze_time("2000-01-01"):
        user_manifest = remote_to_local_manifest(new_workspace_manifest(first_device_id))
    user_manifest["base_version"] = 1
    user_manifest_access = new_access()

    devices = []
    for device_name in devices_names:
        device_signkey = SigningKey.generate().encode()
        device = Device(
            "%s@%s" % (user_id, device_name),
            user_privkey,
            device_signkey,
            user_manifest_access,
            InMemoryLocalDB(),
        )
        device.local_db.set(user_manifest_access, user_manifest)
        devices.append(device)

    return devices


def connect_signal_as_event(signal_ns, signal_name):
    event = trio.Event()
    callback = Mock(spec_set=())
    callback.side_effect = lambda *args, **kwargs: event.set()

    event.cb = callback  # Prevent weakref destruction
    signal_ns.signal(signal_name).connect(callback, weak=True)
    return event
