import pytest
from pendulum import Pendulum

from parsec.core.fs.local_fs import LocalManifestFS, FSInvalidPath
from parsec.core.fs.data import new_access
from parsec.core.local_db import LocalDBMissingEntry

from tests.common import freeze_time


@pytest.fixture
def local_manifest_fs(alice, signal_ns):
    return LocalManifestFS(alice)


def test_stat_root(local_manifest_fs):
    stat = local_manifest_fs.stat("/")
    assert stat == {
        "type": "folder",
        "base_version": 1,
        "is_placeholder": False,
        "need_sync": False,
        "created": Pendulum(2000, 1, 1),
        "updated": Pendulum(2000, 1, 1),
        "children": [],
    }


def test_file_create(local_manifest_fs):
    with freeze_time('2000-01-02'):
        local_manifest_fs.file_create("/foo.txt")

    root_stat = local_manifest_fs.stat("/")
    assert root_stat == {
        "type": "folder",
        "base_version": 1,
        "is_placeholder": False,
        "need_sync": True,
        "created": Pendulum(2000, 1, 1),
        "updated": Pendulum(2000, 1, 2),
        "children": ['foo.txt'],
    }

    foo_stat = local_manifest_fs.stat("/foo.txt")
    assert foo_stat == {
        "type": "file",
        "base_version": 0,
        "is_placeholder": True,
        "need_sync": True,
        "created": Pendulum(2000, 1, 2),
        "updated": Pendulum(2000, 1, 2),
        "size": 0,
    }


def test_access_not_loaded_entry(alice, local_manifest_fs):
    user_manifest = alice.local_db.get(alice.user_manifest_access)
    user_manifest['children']['foo.txt'] = new_access()
    alice.local_db.set(alice.user_manifest_access, user_manifest)

    with pytest.raises(LocalDBMissingEntry):
        local_manifest_fs.stat("/foo.txt")


def test_access_unknown_entry(local_manifest_fs):
    with pytest.raises(FSInvalidPath):
        local_manifest_fs.stat("/dummy")
