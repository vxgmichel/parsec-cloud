import pytest
import zmq

from tests.backend.conftest import SERVER_PUBLIC


class TestBadAuth:
    DUMMYKEY = b"CzXP)X*eM0<=*fz0#>oW=NGcf?Nb<XLQ3D{G{6A}"

    def test_no_key(self, backend_addr):
        ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(backend_addr)
        self._testbed(socket, False)

    @pytest.mark.parametrize('keys', [
        {'CLIENT_SECRET': DUMMYKEY},
        {'CLIENT_PUBLIC': DUMMYKEY},
        {'SERVER_PUBLIC': DUMMYKEY},
    ])
    def test_bad_key(self, backend_addr, keys, alice):
        alice_private = zmq.utils.z85.encode(alice['private'])
        alice_public = zmq.utils.z85.encode(alice['public'])
        ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.REQ)
        socket.curve_secretkey = keys.get('CLIENT_SECRET', alice_private)
        socket.curve_publickey = keys.get('CLIENT_PUBLIC', alice_public)
        socket.curve_serverkey = keys.get('SERVER_PUBLIC', SERVER_PUBLIC)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(backend_addr)
        self._testbed(socket, False)

    def _testbed(self, socket, auth_ok):
        # Server should never answer given connection failed
        # TODO: client should configure ERRNO at C level once the connection
        # is refused, find a way to access&check it
        socket.send(b'foo')
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        if poller.poll(200):  # 200ms timeout
            msg = socket.recv()
            if auth_ok:
                return msg
            else:
                raise AssertionError("Backend accept connection and send back message: %r" % msg)
        elif auth_ok:
            raise AssertionError("Backend didn't accept connection")

    def test_good_auth(self, backend_addr, alice):
        alice_private = zmq.utils.z85.encode(alice['private'])
        alice_public = zmq.utils.z85.encode(alice['public'])
        ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.REQ)
        socket.curve_secretkey = alice_private
        socket.curve_publickey = alice_public
        socket.curve_serverkey = SERVER_PUBLIC
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(backend_addr)
        rep = self._testbed(socket, True)
        assert rep == b'{"status": "bad_msg"}'


# TODO: test ZAP message handler component alone
# def test_authenticator():
#     CustomAuthenticatorThreadAuthenticator()
#     class MockAuthenticator(BaseDynamicCurveKeysAuthenticator):
#         def _retrieve_identity(self, domain, client_key):
#             return True, 'OK'
