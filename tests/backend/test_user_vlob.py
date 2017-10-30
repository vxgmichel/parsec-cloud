import pytest


class TestUserVlobComponent:

    def test_user_vlob_read_ok(self, alicesock):
        alicesock.send({'cmd': 'user_vlob_read'})
        rep, content = alicesock.recv()
        assert rep == {'status': 'ok', 'version': 0}
        assert content == b''

    def test_user_vlob_read_wrong_version(self, alicesock):
        alicesock.send({'cmd': 'user_vlob_read', 'version': 42})
        rep, _ = alicesock.recv()
        assert rep == {'status': 'user_vlob_error', 'label': 'Wrong blob version.'}

    def test_user_vlob_update_ok_and_read_previous_version(self, backend, alicesock):
        # Update user vlob
        alicesock.send({'cmd': 'user_vlob_update', 'version': 1}, b'Next version.')
        rep, _ = alicesock.recv()
        assert rep == {'status': 'ok'}
        # Read previous version
        alicesock.send({'cmd': 'user_vlob_read', 'version': 0})
        rep, content = alicesock.recv()
        assert rep == {'status': 'ok', 'version': 0}
        assert content == b''
        # New version should be ok as well
        alicesock.send({'cmd': 'user_vlob_read'})
        rep, content = alicesock.recv()
        assert rep == {'status': 'ok', 'version': 1}
        assert content == b'Next version.'

    def test_user_vlob_update_wrong_version(self, alicesock):
        alicesock.send({'cmd': 'user_vlob_update', 'version': 42}, b'Next version.')
        rep, _ = alicesock.recv()
        assert rep == {'status': 'user_vlob_error', 'label': 'Wrong blob version.'}

    def test_multiple_users(self, alicesock, bobsock):
        def _update(username, usersock):
            usersock.send(
                {'cmd': 'user_vlob_update', 'version': 1},
                b'Next version for %s.' % username.encode()
            )

        _update('alice', alicesock)
        _update('bob', bobsock)
        alicerep, _ = alicesock.recv()
        bobrep, _ = bobsock.recv()
        assert alicerep == {'status': 'ok'}
        assert bobrep == {'status': 'ok'}

        alicesock.send({'cmd': 'user_vlob_read', 'version': 1})
        bobsock.send({'cmd': 'user_vlob_read', 'version': 1})
        alicerep, alicecontent = alicesock.recv()
        bobrep, bobcontent = bobsock.recv()

        assert alicerep == {'status': 'ok', 'version': 1}
        assert alicecontent == b'Next version for alice.'
        assert bobrep == {'status': 'ok', 'version': 1}
        assert bobcontent == b'Next version for bob.'


# TODO: test event
# TODO: test can't listen other user's events
