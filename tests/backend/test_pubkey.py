import pytest


def _build_from_template(template, bases):
    out = template.copy()
    for base_field, base_value in bases.items():
        for field, value in out.items():
            if value == '<%s>' % base_field:
                out[field] = base_value
    return out


class TestPubKey:

    def test_pubkey_get_ok(self, alicesock, bob):
        alicesock.send({'cmd': 'pubkey_get', 'id': bob['id']})
        rep = alicesock.recv()
        assert rep == {
            'status': 'ok',
            'id': bob['id'],
            'key': bob['public']
        }

    @pytest.mark.parametrize('msg', [
        # Bad id
        {'id': 42},
        {'id': None},
        # Bad Field
        {'id': 'bob@test.com', 'unknown': 'field'},
        {}
    ])
    def test_pubkey_get_bad_msg(self, msg, alicesock, bob):
        alicesock.send({'cmd': 'pubkey_get', **msg})
        rep = alicesock.recv()
        assert rep['status'] == 'bad_msg'

    def test_pubkey_get_not_found(self, alicesock):
        alicesock.send({'cmd': 'pubkey_get', 'id': 'dummy@test.com'})
        rep = alicesock.recv()
        assert rep == {'status': 'pubkey_not_found', 'label': 'Unknown user'}
