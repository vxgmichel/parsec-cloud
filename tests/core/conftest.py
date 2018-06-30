import pytest

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
async def fs(nursery, alice, manifests_manager, blocks_manager):
    fs = FS(alice, manifests_manager, blocks_manager)
    await fs.init(nursery)
    return fs
