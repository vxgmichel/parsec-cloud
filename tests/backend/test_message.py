import pytest


def _build_from_template(template, bases):
    out = template.copy()
    for base_field, base_value in bases.items():
        for field, value in out.items():
            if value == '<%s>' % base_field:
                out[field] = base_value
    return out


@pytest.fixture
def _sendmsg(sendersock, recipient, msg):
    sendersock.send({'cmd': 'message_new', 'recipient': recipient}, msg)
    rep = sendersock.recv()
    assert rep == {'status': 'ok'}


class TestMessage:

    def test_message_get_ok(self, alicesock, alice, bobsock):
        _sendmsg(bobsock, alice['id'], b'zero')
        _sendmsg(bobsock, alice['id'], b'one')
        _sendmsg(bobsock, alice['id'], b'two')
        alicesock.send({'cmd': 'message_get'})
        rep, *messagesframes = alicesock.recv(exframes=True)
        assert rep == {'status': 'ok', 'count': 3, 'offset': 0}
        assert messagesframes == [b'zero', b'one', b'two']

    def test_message_get_with_offset(self, alicesock, alice, bobsock):
        _sendmsg(bobsock, alice['id'], b'zero')
        _sendmsg(bobsock, alice['id'], b'one')
        _sendmsg(bobsock, alice['id'], b'two')
        _sendmsg(bobsock, alice['id'], b'three')
        alicesock.send({'cmd': 'message_get', 'offset': 1, 'limit': 2})
        rep, *messagesframes = alicesock.recv(exframes=True)
        assert rep == {'status': 'ok', 'count': 2, 'offset': 1}
        assert messagesframes == [b'one', b'two']

    @pytest.mark.parametrize('msg', [
        {'bad_field': 'foo'},
        {'offset': 0, 'bad_field': 'foo'},
        {'offset': None},
        {'offset': 'zero'},
        {'offset': -1},
        {'limit': 'zero'},
        {'limit': -1},
    ])
    def test_message_get_bad_msg(self, msg, alicesock, alice, bobsock):
        _sendmsg(bobsock, alice['id'], b'zero')
        _sendmsg(bobsock, alice['id'], b'one')
        _sendmsg(bobsock, alice['id'], b'two')
        _sendmsg(bobsock, alice['id'], b'three')
        alicesock.send({'cmd': 'message_get', **msg})
        rep = alicesock.recv()
        assert rep['status'] == 'bad_msg'

    def test_message_new_ok(self, alicesock, bob, bobsock):
        alicesock.send({'cmd': 'message_new', 'recipient': bob['id']}, b'Hi from alice !')
        rep = alicesock.recv()
        assert rep == {'status': 'ok'}
        # Try to get it as bob
        bobsock.send({'cmd': 'message_get'})
        rep, *messagesframes = bobsock.recv(exframes=True)
        assert rep == {'status': 'ok', 'offset': 0, 'count': 1}
        assert messagesframes == [b'Hi from alice !']
        # Off course alice should not get the message !
        alicesock.send({'cmd': 'message_get'})
        rep, *messagesframes = alicesock.recv(exframes=True)
        assert rep == {'status': 'ok', 'offset': 0, 'count': 0}
        assert not messagesframes

    @pytest.mark.parametrize('msg,content', [
        ({'recipient': '<id>', 'bad_field': 'foo'}, b'hello'),
        ({'recipient': '<id>'}, None),
        # ({'recipient': '<id>'}, (b'too', b'many', b'frames')),
        ({'recipient': 42}, b'hello'),
        ({'recipient': None}, b'hello'),
        ({}, b'hello'),
    ])
    def test_message_new_bad_msg(self, msg, content, alicesock, bob):
        msg = _build_from_template(msg, bob)
        if content is None:
            contentframes = []
        elif isinstance(content, (list, tuple)):
            contentframes = content
        else:
            contentframes = [content]
        alicesock.send({'cmd': 'message_new', **msg}, *contentframes)
        rep = alicesock.recv()
        assert rep['status'] in ('bad_msg', 'missing_body_frame')
