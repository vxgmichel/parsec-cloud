import pytest
import json


class TestUserVlobComponent:

    def test_user_vlob_read_ok(self, alicesock):
        alicesock.send_json({'cmd': 'user_vlob_read'})
        repframes = alicesock.recv_multipart()
        assert len(repframes) == 2, repframes
        rep = json.loads(repframes[0])
        content = repframes[1]
        assert rep == {'status': 'ok', 'version': 0}
        assert content == b''

    def test_user_vlob_read_wrong_version(self, alicesock):
        alicesock.send_json({'cmd': 'user_vlob_read', 'version': 42})
        rep = alicesock.recv_json()
        assert rep == {'status': 'user_vlob_error', 'label': 'Wrong blob version.'}

    def test_user_vlob_update_ok_and_read_previous_version(self, backend, alicesock):
        # Update user vlob
        rawreq = json.dumps({'cmd': 'user_vlob_update', 'version': 1}).encode()
        alicesock.send_multipart((rawreq, b'Next version.'))
        rep = alicesock.recv_json()
        assert rep == {'status': 'ok'}
        # Read previous version
        alicesock.send_json({'cmd': 'user_vlob_read', 'version': 0})
        repframes = alicesock.recv_multipart()
        assert len(repframes) == 2, repframes
        rep = json.loads(repframes[0])
        content = repframes[1]
        assert rep == {'status': 'ok', 'version': 0}
        assert content == b''
        # New version should be ok as well
        alicesock.send_json({'cmd': 'user_vlob_read'})
        repframes = alicesock.recv_multipart()
        assert len(repframes) == 2, repframes
        rep = json.loads(repframes[0])
        content = repframes[1]
        assert rep == {'status': 'ok', 'version': 1}
        assert content == b'Next version.'

    def test_user_vlob_update_wrong_version(self, alicesock):
        rawreq = json.dumps({'cmd': 'user_vlob_update', 'version': 42}).encode()
        alicesock.send_multipart((rawreq, b'Next version.'))
        rep = alicesock.recv_json()
        assert rep == {'status': 'user_vlob_error', 'label': 'Wrong blob version.'}

    def test_multiple_users(self, alicesock, bobsock):
        def _update(username, usersock):
            rawreq = json.dumps({'cmd': 'user_vlob_update', 'version': 1}).encode()
            usersock.send_multipart((rawreq, b'Next version for %s.' % username.encode()))

        _update('alice', alicesock)
        _update('bob', bobsock)
        alicerep = alicesock.recv_json()
        bobrep = bobsock.recv_json()
        assert alicerep == {'status': 'ok'}
        assert bobrep == {'status': 'ok'}

        alicesock.send_json({'cmd': 'user_vlob_read', 'version': 1})
        bobsock.send_json({'cmd': 'user_vlob_read', 'version': 1})
        alicerep = alicesock.recv_multipart()
        bobrep = bobsock.recv_multipart()

        def _check_rep(username, repframes):
            assert len(repframes) == 2, repframes
            rep = json.loads(repframes[0])
            content = repframes[1]
            assert rep == {'status': 'ok', 'version': 1}
            assert content == b'Next version for %s.' % username.encode()

        _check_rep('alice', alicerep)
        _check_rep('bob', bobrep)


# TODO: test event
# TODO: test can't listen other user's events
