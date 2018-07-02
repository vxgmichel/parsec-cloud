import trio_asyncio
import os
import pytest
import attr
import socket
import asyncpg
import contextlib
from unittest.mock import patch
import hypothesis

from parsec.signals import SignalsContext
from parsec.backend.exceptions import AlreadyExistsError as UserAlreadyExistsError
from parsec.backend.drivers import postgresql as pg_driver

from tests.common import (
    freeze_time,
    run_app,
    backend_factory,
    core_factory,
    connect_backend,
    connect_core,
    bootstrap_devices,
    bootstrap_device,
)
from tests.open_tcp_stream_mock_wrapper import OpenTCPStreamMockWrapper


def pytest_addoption(parser):
    parser.addoption("--hypothesis-max-examples", default=100, type=int)
    parser.addoption("--hypothesis-derandomize", action="store_true")
    parser.addoption(
        "--no-postgresql", action="store_true", help="Don't run tests making use of PostgreSQL"
    )
    parser.addoption(
        "--only-postgresql", action="store_true", help="Only run tests making use of PostgreSQL"
    )
    parser.addoption("--runslow", action="store_true", help="Don't skip slow tests")


@pytest.fixture
def hypothesis_settings(request):
    return hypothesis.settings(
        max_examples=pytest.config.getoption("--hypothesis-max-examples"),
        derandomize=pytest.config.getoption("--hypothesis-derandomize"),
    )


def pytest_runtest_setup(item):
    # Mock and non-UTC timezones are a really bad mix, so keep things simple
    os.environ.setdefault("TZ", "UTC")
    if "slow" in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run")


# Use current unix user's credential, don't forget to do
# `psql -c 'CREATE DATABASE parsec_test;'` prior to run tests
DEFAULT_POSTGRESQL_TEST_URL = "postgresql:///parsec_test"

# Use current unix user's credential, don't forget to do
# `psql -c 'CREATE DATABASE triopg_test;'` prior to run tests
TRIOPG_POSTGRESQL_TEST_URL = "postgresql:///triopg_test"


def get_postgresql_url():
    return os.environ.get("PARSEC_POSTGRESQL_TEST_URL", DEFAULT_POSTGRESQL_TEST_URL)


@pytest.fixture
def postgresql_url(request):
    if pytest.config.getoption("--no-postgresql"):
        pytest.skip("`--no-postgresql` option provided")
    return get_postgresql_url()


@pytest.fixture
async def asyncio_loop():
    async with trio_asyncio.open_loop() as loop:
        yield loop


@pytest.fixture(params=["mocked", "postgresql"])
async def backend_store(request, asyncio_loop):
    if request.param == "postgresql":
        if pytest.config.getoption("--no-postgresql"):
            pytest.skip("`--no-postgresql` option provided")
        url = get_postgresql_url()
        try:
            await pg_driver.handler.init_db(url, True)
        except asyncpg.exceptions.InvalidCatalogNameError as exc:
            raise RuntimeError(
                "Is `parsec_test` a valid database in PostgreSQL ?\n"
                "Running `psql -c 'CREATE DATABASE parsec_test;'` may fix this"
            ) from exc
        return url

    else:
        if pytest.config.getoption("--only-postgresql"):
            pytest.skip("`--only-postgresql` option provided")
        return "mocked://"


@pytest.fixture
def alice_devices():
    return bootstrap_devices("alice", ("dev1", "dev2"))


@pytest.fixture
def alice(alice_devices):
    return alice_devices[0]


@pytest.fixture
def alice2(tmpdir):
    return alice_devices[1]


@pytest.fixture
def bob(tmpdir):
    return bootstrap_device("bob", "dev1")


@pytest.fixture
def mallory(tmpdir):
    return bootstrap_device("mallory", "dev1")


@pytest.fixture
def always_logs():
    """
    By default, pytest-logbook only print last test's logs in case of error.
    With this fixture all logs are outputed as soon as they are created.
    """
    from logbook import StreamHandler
    import sys

    sh = StreamHandler(sys.stdout)
    with sh.applicationbound():
        yield


@pytest.fixture
def unused_tcp_port():
    """Find an unused localhost TCP port from 1024-65535 and return it."""
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def unused_tcp_addr(unused_tcp_port):
    return "tcp://127.0.0.1:%s" % unused_tcp_port


@pytest.fixture
def signal_ns():
    with SignalsContext() as ctx:
        yield ctx.signals_namespace


@pytest.fixture
def default_devices(alice, alice2, bob):
    return (alice, alice2, bob)


@pytest.fixture
async def backend(default_devices, backend_store, config={}):
    async with backend_factory(
        **{"blockstore_postgresql": True, "dburl": backend_store, **config}
    ) as backend:

        with freeze_time("2000-01-01"):
            for device in default_devices:
                try:
                    await backend.user.create(
                        author="<backend-fixture>",
                        user_id=device.user_id,
                        broadcast_key=device.user_pubkey.encode(),
                        devices=[(device.device_name, device.device_verifykey.encode())],
                    )
                except UserAlreadyExistsError:
                    await backend.user.create_device(
                        user_id=device.user_id,
                        device_name=device.device_name,
                        verify_key=device.device_verifykey.encode(),
                    )

        yield backend


@pytest.fixture
def backend_addr(tcp_stream_spy):
    return "tcp://placeholder.com:9999"


@pytest.fixture
def tcp_stream_spy():
    open_tcp_stream_mock_wrapper = OpenTCPStreamMockWrapper()
    with patch("trio.open_tcp_stream", new=open_tcp_stream_mock_wrapper):
        yield open_tcp_stream_mock_wrapper


@attr.s(frozen=True)
class RunningBackendInfo:
    backend = attr.ib()
    addr = attr.ib()
    connection_factory = attr.ib()


@pytest.fixture
async def running_backend(tcp_stream_spy, backend, backend_addr):
    async with run_app(backend) as backend_connection_factory:
        with tcp_stream_spy.install_hook(backend_addr, backend_connection_factory):
            yield RunningBackendInfo(backend, backend_addr, backend_connection_factory)


@pytest.fixture
async def alice_backend_sock(backend, alice):
    async with connect_backend(backend, auth_as=alice) as sock:
        yield sock


@pytest.fixture
async def bob_backend_sock(backend, bob):
    async with connect_backend(backend, auth_as=bob) as sock:
        yield sock


@pytest.fixture
def core_signal_ns(core):
    return core.signals_context.signals_namespace


@pytest.fixture
async def core(asyncio_loop, backend_addr, tmpdir, default_devices, config={}):
    async with core_factory(
        **{
            "base_settings_path": tmpdir.mkdir("core_fixture").strpath,
            "backend_addr": backend_addr,
            **config,
        }
    ) as core:

        for device in default_devices:
            core.devices_manager.register_new_device(
                device.id, device.user_privkey.encode(), device.device_signkey.encode(), "<secret>"
            )

        yield core


@pytest.fixture
async def core2(asyncio_loop, backend_addr, tmpdir, default_devices, config={}):
    # TODO: refacto with core fixture
    async with core_factory(
        **{
            "base_settings_path": tmpdir.mkdir("core2_fixture").strpath,
            "backend_addr": backend_addr,
            **config,
        }
    ) as core:

        for device in default_devices:
            core.devices_manager.register_new_device(
                device.id, device.user_privkey.encode(), device.device_signkey.encode(), "<secret>"
            )

        yield core


@pytest.fixture
async def alice_core_sock(core, alice):
    assert not core.auth_device, "Core already logged"
    async with connect_core(core) as sock:
        await core.login(alice)
        yield sock


@pytest.fixture
async def alice2_core2_sock(core2, alice2):
    assert not core2.auth_device, "Core already logged"
    async with connect_core(core2) as sock:
        await core2.login(alice2)
        yield sock


@pytest.fixture
async def bob_core2_sock(core2, bob):
    assert not core2.auth_device, "Core already logged"
    async with connect_core(core2) as sock:
        await core2.login(bob)
        yield sock


@pytest.fixture
def monitor():
    from tests.monitor import Monitor

    return Monitor()
