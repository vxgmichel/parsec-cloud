import pytest

from parsec.signals import Namespace as SignalNamespace
from parsec.core.local_storage import LocalStorage
from parsec.core.backend_storage import BackendStorage
from parsec.core.backend_cmds_sender import BackendCmdsSender
from parsec.core.encryption_manager import EncryptionManager
from parsec.core.manifests_manager import ManifestsManager
from parsec.core.blocks_manager import BlocksManager
from parsec.core.fs import FS


@pytest.fixture
async def backend_connections_multiplexer(nursery, alice, running_backend):
    bcm = BackendCmdsSender(alice, running_backend.addr)
    await bcm.init(nursery)
    return bcm


@pytest.fixture
async def local_storage(nursery, tmpdir):
    ls = LocalStorage(tmpdir / "local_storage-fixture")
    await ls.init(nursery)
    return ls


@pytest.fixture
async def backend_storage(nursery, backend_connections_multiplexer):
    bs = BackendStorage(backend_connections_multiplexer)
    await bs.init(nursery)
    return bs


@pytest.fixture
async def encryption_manager(nursery, alice, backend_connections_multiplexer, local_storage):
    em = EncryptionManager(alice, backend_connections_multiplexer, local_storage)
    await em.init(nursery)
    return em


@pytest.fixture
async def manifests_manager(nursery, local_storage, backend_storage, encryption_manager):
    mm = ManifestsManager(local_storage, backend_storage, encryption_manager)
    await mm.init(nursery)
    return mm


@pytest.fixture
async def blocks_manager(nursery, local_storage, backend_storage):
    bm = BlocksManager(local_storage, backend_storage)
    await bm.init(nursery)
    return bm


@pytest.fixture
def backend_cmds_sender_factory(nursery, running_backend):
    async def _backend_cmds_sender_factory(device, backend_addr=None):
        if not backend_addr:
            backend_addr = running_backend.addr
        bcs = BackendCmdsSender(device, backend_addr)
        await bcs.init(nursery)
        return bcs

    return _backend_cmds_sender_factory


@pytest.fixture
def fs_factory(nursery, backend_cmds_sender_factory, signal_ns_factory):
    async def _fs_factory(device, backend_addr=None, signal_ns=None):
        if not signal_ns:
            signal_ns = signal_ns_factory()
        backend_cmds_sender = await backend_cmds_sender_factory(device, backend_addr=backend_addr)
        fs = FS(device, backend_cmds_sender, signal_ns)
        await fs.init(nursery)
        return fs

    return _fs_factory


@pytest.fixture
def signal_ns_factory():
    return SignalNamespace


@pytest.fixture
async def backend_cmds_sender(alice):
    return backend_cmds_sender_factory(alice)


@pytest.fixture
async def fs(fs_factory, alice):
    return fs_factory(alice)


@pytest.fixture
def backend_addr_factory(running_backend, tcp_stream_spy):
    # Creating new addr for backend make it easy be selective on what to
    # turn offline
    counter = 0

    def _backend_addr_factory():
        nonlocal counter
        addr = f"tcp://{counter}.placeholder.com:9999"
        tcp_stream_spy.push_hook(addr, running_backend.connection_factory)
        counter += 1
        return addr

    return _backend_addr_factory
