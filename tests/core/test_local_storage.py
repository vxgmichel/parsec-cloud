import pytest

from parsec.core.local_storage import LocalStorage


@pytest.fixture
async def local_storage(nursery):
    ls = LocalStorage(":memory:")
    await ls.init(nursery)
    return ls


@pytest.mark.trio
async def test_fetch_user_manifest_not_available(local_storage):
    blob = local_storage.fetch_user_manifest()
    assert blob is None


@pytest.mark.trio
async def test_flush_and_fetch_user_manifest(local_storage):
    local_storage.flush_user_manifest(b"<user manifest>")
    blob = local_storage.fetch_user_manifest()
    assert blob == b"<user manifest>"


@pytest.mark.trio
async def test_fetch_manifest_not_available(local_storage):
    blob = local_storage.fetch_manifest("<unknown_id>")
    assert blob is None


@pytest.mark.trio
async def test_flush_and_fetch_manifest(local_storage):
    data = [("<id#%s>" % i, ("<manifest#%s>" % i).encode()) for i in range(3)]
    for id, blob in data:
        local_storage.flush_manifest(id, blob)
    for id, expected_blob in data:
        blob = local_storage.fetch_manifest(id)
        assert blob == expected_blob


@pytest.mark.trio
async def test_move_manifest_manifest(local_storage):
    local_storage.flush_manifest("<id#1>", b"<manifest>")
    local_storage.move_manifest("<id#1>", "<id#2>")
    old = local_storage.fetch_manifest("<id#1>")
    assert old is None
    new = local_storage.fetch_manifest("<id#2>")
    assert new == b"<manifest>"


@pytest.mark.trio
async def test_flush_and_fetch_device_verify_key(local_storage):
    data = [("<user_id#%s>" % i, "<device_name#%s>" % i, ("<verify_key#%s>" % i).encode()) for i in range(3)]
    for user_id, device_name, verify_key in data:
        local_storage.flush_device_verify_key(user_id, device_name, verify_key)
    for user_id, device_name, expected_verify_key in data:
        verify_key = local_storage.fetch_device_verify_key(user_id, device_name)
        assert verify_key == expected_verify_key
