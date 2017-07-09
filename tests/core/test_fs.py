import asyncio
import pytest
from unittest.mock import Mock
from effect2 import do, Effect
from effect2.testing import perform_sequence, const
from freezegun import freeze_time

from parsec.core.app import App, app_factory
from parsec.core.fs import (FSComponent, ESynchronize, EGroupCreate, EDustbinShow, EManifestHistory,
                            EManifestRestore, EFileCreate, EFileRead, EFileWrite, EFileTruncate,
                            EFileHistory, EFileRestore, EFolderCreate, EStat, EMove, EDelete,
                            EUndelete)
from parsec.core.privkey import EPrivkeyAdd, EPrivkeyGet, EPrivkeyLoad, PrivKeyComponent
from parsec.core.identity import EIdentityLoad, Identity
from parsec.core.synchronizer import (
    EUserVlobSynchronize, EUserVlobRead, EUserVlobUpdate, EVlobCreate, EVlobList, EVlobRead,
    EVlobUpdate, EVlobDelete, EVlobSynchronize, EBlockCreate, EBlockDelete, EBlockSynchronize,
    SynchronizerComponent)
from parsec.exceptions import ManifestError, ManifestNotFound, BlockNotFound, VlobNotFound
from parsec.tools import ejson_dumps, ejson_loads, to_jsonb64, from_jsonb64, digest
from tests.test_crypto import ALICE_PRIVATE_RSA, mock_crypto_passthrough


@pytest.fixture
def app(mock_crypto_passthrough):
    loop = asyncio.get_event_loop()
    app = FSComponent()
    privkey_component = PrivKeyComponent()
    fs_component = FSComponent()
    synchronizer_component = SynchronizerComponent()
    app = app_factory(
        privkey_component.get_dispatcher(), fs_component.get_dispatcher(),
        synchronizer_component.get_dispatcher())

    @do
    def load_identity():
        yield Effect(EIdentityLoad('ALICE', ALICE_PRIVATE_RSA))
    loop.run_until_complete(app.async_perform(load_identity()))
    blob = {'dustbin': [], 'entries': {'/': None}, 'groups': {}, 'versions': {}}
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    sequence = [
        (EUserVlobRead(),
            lambda _: {'blob': '', 'version': 0}),
        (EUserVlobUpdate(1, blob),
            lambda _: None)
    ]
    perform_sequence(sequence, fs_component.on_app_start(app))
    return fs_component


@pytest.fixture
def file(app, mock_crypto_passthrough):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    block_id = '4567'
    blob = [{'blocks': [{'block': block_id, 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_file_create(EFileCreate('/foo'))
    sequence = [
        (EBlockCreate(''),
            lambda _: block_id),
        (EVlobCreate(blob),
            lambda _: vlob)
    ]
    ret = perform_sequence(sequence, eff)
    assert ret is None


def raise_(exception):
    raise exception


def test_perform_synchronize(app):
    blob = {'dustbin': [], 'entries': {'/': None}, 'groups': {}, 'versions': {}}
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_synchronize(ESynchronize())
    sequence = [
        (EVlobList(),
            lambda _: []),
        (EUserVlobUpdate(1, blob),
            lambda _: None),
        (EUserVlobSynchronize(),
            lambda _: None)
    ]
    ret = perform_sequence(sequence, eff)
    assert ret is None


def test_perform_group_create(app):
    blob = {'dustbin': [], 'entries': {'/': None}, 'versions': {}}
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_group_create(EGroupCreate('share'))
    sequence = [
        (EVlobCreate(),
            lambda _: {'id': '1234', 'read_trust_seed': '42', 'write_trust_seed': '43'}),
        (EVlobUpdate('1234', '43', 1, blob),
            lambda _: None)
    ]
    ret = perform_sequence(sequence, eff)
    assert ret is None


def test_perform_dustbin_show(app, file):
    with freeze_time('2012-01-01') as frozen_datetime:
        vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
        blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
                 'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
        blob = ejson_dumps(blob).encode()
        blob = to_jsonb64(blob)
        eff = app.perform_delete(EDelete('/foo'))
        sequence = [
            (EVlobRead(vlob['id'], vlob['read_trust_seed']),
                lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
            (EVlobList(),
                lambda _: []),
            (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
                lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
            (EBlockDelete('4567'),
                lambda _: raise_(BlockNotFound('Block not found.'))),
            (EVlobDelete('2345'),
                lambda _: raise_(VlobNotFound('Vlob not found.'))),
        ]
        perform_sequence(sequence, eff)
        eff = app.perform_dustbin_show(EDustbinShow())
        dustbin = perform_sequence([], eff)
        vlob['path'] = '/foo'
        vlob['removed_date'] = frozen_datetime().isoformat()
        vlob['key'] = to_jsonb64(b'<dummy-key-00000000000000000002>')
        assert dustbin == [vlob]


def test_perform_manifest_history(app):
    eff = app.perform_manifest_history(EManifestHistory())
    history = perform_sequence([], eff)
    assert history == {'detailed_history': []}


def test_perform_manifest_restore(app):
    eff = app.perform_manifest_restore(EManifestRestore())
    with pytest.raises(ManifestError):
        perform_sequence([], eff)


def test_perform_file_create(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    block_id = '4567'
    # Already exist
    blob = [{'blocks': [{'block': block_id, 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000003>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_file_create(EFileCreate('/foo'))
    sequence = [
        (EBlockCreate(''),
            lambda _: block_id),
        (EVlobCreate(blob),
            lambda _: vlob),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EBlockDelete(block_id),
            lambda _: None),
        (EVlobDelete(vlob['id']),
            lambda _: None)
    ]
    with pytest.raises(ManifestError):
        perform_sequence(sequence, eff)


def test_perform_file_read(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_file_read(EFileRead('/foo'))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobList(),
            lambda _: [vlob['id']]),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
    ]
    file = perform_sequence(sequence, eff)
    assert file == b''


def test_perform_file_write(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    new_blob = [{'blocks': [{'block': '4567',
                             'digest': digest(''),
                             'size': len('')}],
                 'key': to_jsonb64(b'<dummy-key-00000000000000000001>')},
                {'blocks': [{'block': '5678',
                             'digest': digest(to_jsonb64(b'foo')),
                             'size': 3}],
                 'key': to_jsonb64(b'<dummy-key-00000000000000000003>')}]
    new_blob = ejson_dumps(new_blob).encode()
    new_blob = to_jsonb64(new_blob)
    eff = app.perform_file_write(EFileWrite('/foo', b'foo', 0))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobList(),
            lambda _: [vlob['id']]),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EBlockCreate(to_jsonb64(b'foo')),
            lambda _: '5678'),
        (EVlobUpdate(vlob['id'], vlob['write_trust_seed'], 1, new_blob),
            lambda _: None),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': new_blob, 'version': 1})
    ]
    ret = perform_sequence(sequence, eff)
    assert ret is None


def test_perform_file_truncate(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    new_blob = [{'blocks': [{'block': '5678',
                             'digest': digest(''),
                             'size': len('')}],
                 'key': to_jsonb64(b'<dummy-key-00000000000000000003>')}]
    new_blob = ejson_dumps(new_blob).encode()
    new_blob = to_jsonb64(new_blob)
    eff = app.perform_file_truncate(EFileTruncate('/foo', 0))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobList(),
            lambda _: [vlob['id']]),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EBlockCreate(to_jsonb64(b'')),
            lambda _: '5678'),
        (EVlobUpdate(vlob['id'], vlob['write_trust_seed'], 1, new_blob),
            lambda _: None),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': new_blob, 'version': 1}),
        (EBlockDelete('4567'),
            lambda _: None)
    ]
    ret = perform_sequence(sequence, eff)
    assert ret is None


@pytest.mark.xfail
def test_perform_file_history(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_file_history(EFileHistory('/foo', 1, 1))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobList(),
            lambda _: []),
    ]
    perform_sequence(sequence, eff)


def test_perform_file_restore(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_file_restore(EFileRestore('/foo'))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 2}),
        (EVlobList(),
            lambda _: []),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 2),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 2}),
        (EBlockDelete('4567'),
            lambda _: None),
        (EVlobDelete(vlob['id']),
            lambda _: None),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobUpdate(vlob['id'], vlob['write_trust_seed'], 3, blob),
            lambda _: None),
    ]
    perform_sequence(sequence, eff)


def test_perform_folder_create(app):
    eff = app.perform_folder_create(EFolderCreate('/dir'))
    ret = perform_sequence([], eff)
    assert ret is None


def test_perform_stat(app, file):
    eff = app.perform_folder_create(EFolderCreate('/dir'))
    ret = perform_sequence([], eff)
    eff = app.perform_stat(EStat('/dir'))
    ret = perform_sequence([], eff)
    assert ret == {'items': [], 'type': 'folder'}


def test_perform_move(app):
    eff = app.perform_folder_create(EFolderCreate('/dir'))
    ret = perform_sequence([], eff)
    eff = app.perform_move(EMove('/dir', '/dir2'))
    ret = perform_sequence([], eff)
    assert ret is None


def test_perform_delete(app):
    eff = app.perform_folder_create(EFolderCreate('/dir'))
    ret = perform_sequence([], eff)
    eff = app.perform_delete(EDelete('/dir'))
    ret = perform_sequence([], eff)
    assert ret is None


def test_perform_undelete(app, file):
    vlob = {'id': '2345', 'read_trust_seed': '42', 'write_trust_seed': '43'}
    blob = [{'blocks': [{'block': '4567', 'digest': digest(b''), 'size': 0}],
             'key': to_jsonb64(b'<dummy-key-00000000000000000001>')}]
    blob = ejson_dumps(blob).encode()
    blob = to_jsonb64(blob)
    eff = app.perform_delete(EDelete('/foo'))
    sequence = [
        (EVlobRead(vlob['id'], vlob['read_trust_seed']),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EVlobList(),
            lambda _: []),
        (EVlobRead(vlob['id'], vlob['read_trust_seed'], 1),
            lambda _: {'id': vlob['id'], 'blob': blob, 'version': 1}),
        (EBlockDelete('4567'),
            lambda _: raise_(BlockNotFound('Block not found.'))),
        (EVlobDelete('2345'),
            lambda _: raise_(VlobNotFound('Vlob not found.'))),
    ]
    ret = perform_sequence(sequence, eff)
    eff = app.perform_undelete(EUndelete('2345'))
    ret = perform_sequence([], eff)
    assert ret is None