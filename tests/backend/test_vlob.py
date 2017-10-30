import pytest
import unittest.mock


@pytest.fixture
def vlob(alicesock, content=b'<content v1>', id=None):
    msg = {'cmd': 'vlob_create'}
    if id:
        msg['id'] = id
    alicesock.send(msg, content)
    rep, _ = alicesock.recv()
    assert rep['status'] == 'ok'
    return {
        'id': rep['id'],
        'content': content,
        'read_trust_seed': rep['read_trust_seed'],
        'write_trust_seed': rep['write_trust_seed'],
        'version': 1
    }


def _build_from_template(template, bases):
    out = template.copy()
    for base_field, base_value in bases.items():
        for field, value in out.items():
            if value == '<%s>' % base_field:
                out[field] = base_value
    return out



class TestVlobAPI:

    @pytest.mark.parametrize("id,blob", [
        (None, b''),
        (None, b'Initial commit.'),
        ('foo', b''),
        ('bar', b'Initial commit.')
    ], ids=lambda x: 'id=%s, blob=%s' % x)
    @unittest.mock.patch('parsec.backend.vlob.generate_vlob_id')
    @unittest.mock.patch('parsec.backend.vlob.generate_trust_seed')
    def test_vlob_create_ok(self, generate_trust_seed, generate_vlob_id, alicesock, id, blob):
        generate_trust_seed.side_effect = ['rts-123', 'wts-456']
        generate_vlob_id.return_value = '#XyZ#'
        msg = {'cmd': 'vlob_create'}
        if id:
            msg['id'] = id
        alicesock.send(msg, blob)
        rep, _ = alicesock.recv()
        assert rep == {
            'status': 'ok',
            'id': id if id else '#XyZ#',
            'read_trust_seed': 'rts-123',
            'write_trust_seed': 'wts-456'
        }

    @pytest.mark.parametrize('msg,content', [
        ({'bad_field': 'foo'}, b'<good content>'),
        ({}, None),  # missing content
        ({'id': 42}, b'<good content>'),  # bad id format
        ({'id': None}, b'<good content>'),  # bad id format
        ({'id': ''}, b'<good content>'),  # Id is 1 long min
        ({'id': 'X' * 33}, b'<good content>'),  # Id is 32 long max
    ])
    def test_vlob_create_bad_msg(self, msg, content, alicesock):
        contentframes = [] if content is None else [content]
        alicesock.send({'cmd': 'vlob_create', **msg}, *contentframes)
        rep, _ = alicesock.recv()
        assert rep['status'] in ('bad_msg', 'missing_body_frame')

    def test_vlob_read_not_found(self, alicesock):
        alicesock.send({'cmd': 'vlob_read', 'id': '1234', 'trust_seed': 'rts-123'})
        rep, _ = alicesock.recv()
        assert rep == {'status': 'vlob_not_found', 'label': 'Vlob not found.'}

    def test_vlob_read_ok(self, alicesock, vlob):
        alicesock.send({
            'cmd': 'vlob_read',
            'id': vlob['id'],
            'trust_seed': vlob['read_trust_seed']
        })
        rep, content = alicesock.recv()
        assert rep == {
            'status': 'ok',
            'id': vlob['id'],
            'version': vlob['version']
        }
        assert content == vlob['content']

    @pytest.mark.parametrize('bad_msg', [
        {'id': '<id>', 'trust_seed': '<read_trust_seed>', 'bad_field': 'foo'},
        {'id': '<id>'},
        {'id': '<id>', 'trust_seed': 42},
        {'id': '<id>', 'trust_seed': None},
        {'id': 42, 'trust_seed': '<read_trust_seed>'},
        {'id': None, 'trust_seed': '<read_trust_seed>'},
        # {'id': '1234567890', 'trust_seed': '<read_trust_seed>'},  # TODO bad?
        {}
    ])
    def test_vlob_read_bad_msg(self, bad_msg, alicesock, vlob):
        msg = _build_from_template(bad_msg, vlob)
        alicesock.send({'cmd': 'vlob_read', **msg})
        rep, _ = alicesock.recv()
        assert rep['status'] in ('bad_msg', 'missing_body_frame')

    def test_read_bad_version(self, alicesock, vlob):
        alicesock.send({
            'cmd': 'vlob_read',
            'id': vlob['id'],
            'trust_seed': vlob['read_trust_seed'],
            'version': 2
        })
        rep, _ = alicesock.recv()
        assert rep['status'] == 'vlob_not_found'

    def test_vlob_update_ok(self, alicesock, vlob):
        new_content = b'<content v2>'
        alicesock.send({
            'cmd': 'vlob_update',
            'id': vlob['id'],
            'trust_seed': vlob['write_trust_seed'],
            'version': 2
        }, new_content)
        rep, _ = alicesock.recv()
        assert rep == {'status': 'ok'}

    @pytest.mark.parametrize('msg,content', [
        # Missing content
        ({'id': '<id>', 'trust_seed': '<write_trust_seed>', 'version': '<version>'}, None),
        # Bad version
        ({'id': '<id>', 'trust_seed': '<write_trust_seed>', 'version': None}, b'content'),
        ({'id': '<id>', 'trust_seed': '<write_trust_seed>', 'version': -1}, b'content'),
        # Bad trust seed
        ({'id': '<id>', 'trust_seed': 'foo', 'version': '<version>'}, b'content'),
        ({'id': '<id>', 'trust_seed': '<read_trust_seed>', 'version': '<version>'}, b'content'),
        ({'id': '<id>', 'trust_seed': None, 'version': '<version>'}, b'content'),
        ({'id': '<id>', 'trust_seed': 42, 'version': '<version>'}, b'content'),
        ({'id': '<id>', 'version': '<version>'}, b'content'),
        # Bad id
        ({'id': 42, 'trust_seed': '<write_trust_seed>', 'version': '<version>'}, b'content'),
        ({'id': None, 'trust_seed': '<write_trust_seed>', 'version': '<version>'}, b'content'),
        ({'trust_seed': '<write_trust_seed>', 'version': '<version>'}, b'content'),
        # Kamoulox
        ({'id': '<id>', 'trust_seed': '<write_trust_seed>',
         'version': '<version>', 'bad_field': 'foo'}, b'content'),
        ({}, b'content')
    ])
    def test_vlob_update_bad_msg(self, msg, content, alicesock, vlob):
        msg = _build_from_template(msg, vlob)
        contentframes = [] if content is None else [content]
        alicesock.send({'cmd': 'vlob_update', **msg}, *contentframes)
        rep, _ = alicesock.recv()
        assert rep['status'] == 'bad_msg'

    def test_vlob_update_bad_id(self, alicesock, vlob):
        new_content = b'<content v2>'
        alicesock.send({
            'cmd': 'vlob_update',
            'id': 'badid',
            'trust_seed': vlob['write_trust_seed'],
            'version': 2
        }, new_content)
        rep, _ = alicesock.recv()
        assert rep == {'status': 'vlob_not_found', 'label': 'Vlob not found.'}

    def test_update_bad_version(self, alicesock, vlob):
        new_content = b'<content v2>'
        alicesock.send({
            'cmd': 'vlob_update',
            'id': vlob['id'],
            'trust_seed': vlob['write_trust_seed'],
            'version': 42
        }, new_content)
        rep, _ = alicesock.recv()
        assert rep == {'status': 'vlob_not_found', 'label': 'Wrong blob version.'}

    def test_update_bad_seed(self, alicesock, vlob):
        new_content = b'<content v2>'
        alicesock.send({
            'cmd': 'vlob_update',
            'id': vlob['id'],
            'trust_seed': 'foooo',
            'version': 2
        }, new_content)
        rep, _ = alicesock.recv()
        assert rep == {'status': 'trust_seed_error', 'label': 'Invalid write trust seed.'}
