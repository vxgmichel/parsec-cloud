import pytest
import zmq
import json
from threading import Thread
from contextlib import contextmanager

from parsec.backend.app import BackendApp
from parsec.core.app import CoreApp
from parsec.tools import b64_to_z85


SERVER_SECRET = 'd4o/C7okJKfllAxHAxtWcDolSXbfKcpdzK0Ppe15M1o='
SERVER_PUBLIC = '0kOyAGdHvgH6EEuhT6WYyvCMABqf9QFlfUOQkOX5M0w='


AVAILABLE_USERS = {
    'alice@test.com': {'private': 'RYZkAjQvy2jxnQy9ksgPdSV4oBrWixbTL7LzNDCXyaE=', 'public': 'YP9vpi2ijHrH9M/FrHeaNvuKgefNiU3Q8USCahTApiM='},
    'bob@test.com': {'private': 'CdrhQlUPX+fRMFlGawb77NuLiNrUAWwdwCnoNpRS99o=', 'public': 'jCgpHEEU0eH9ym8bWp1PoVfo2yu1bQGxGHo9cGpk4F8='},
}


class CookedSock:
    def __init__(self, socket):
        self.socket = socket

    def send(self, msg, *frames):
        self.socket.send_multipart((json.dumps(msg).encode(), *frames))

    def recv(self, exframes=False):
        repframes = self.socket.recv_multipart()
        rep = json.loads(repframes[0].decode())
        repexframes = repframes[1:]
        if not exframes:
            assert not repexframes
            return rep
        else:
            if type(exframes) is int:
                assert len(repexframes) == exframes
            return (rep, *repexframes)


class BackendManager:
    def __init__(self, config):
        self.config = {
            'ZMQ_CONTEXT_FACTORY': zmq.Context,
            'SERVER_PUBLIC': SERVER_PUBLIC,
            'SERVER_SECRET': SERVER_SECRET,
            'TEST_CONTROL_PIPE': 'inproc://backend-control.01',
            'CMDS_SOCKET_URL': 'tcp://127.0.0.1:0',
            'BLOCKSTORE_URL': '<inbackend>',
            **(config or {})
        }
        self.backend = None
        self.ctrl = None
        self.thread = None
        self.addr = None

    def start(self):
        # TODO: Use subprocess instead of daemon ?
        self.backend = BackendApp(self.config)
        self.thread = Thread(target=self.backend.run, daemon=True)
        self.thread.start()
        self.ctrl = self.backend.zmqcontext.socket(zmq.REQ)
        self.ctrl.connect(self.backend.config['TEST_CONTROL_PIPE'])
        self.ctrl.send(b'status')
        rep = self.ctrl.recv()
        assert rep == b'ready'
        # Binded port is dynamically choosen, so extract it from socket
        self.addr = self.backend.cmds_socket.getsockopt(zmq.LAST_ENDPOINT).decode()

    @contextmanager
    def connected(self, userid):
        ctx = zmq.Context()
        socket = ctx.socket(zmq.REQ)
        userkeys = AVAILABLE_USERS[userid]
        socket.curve_secretkey = b64_to_z85(userkeys['private'])
        socket.curve_publickey = b64_to_z85(userkeys['public'])
        socket.curve_serverkey = b64_to_z85(SERVER_PUBLIC)
        socket.connect(self.addr)
        yield CookedSock(socket)
        socket.close()

    def stop(self):
        self.ctrl.send(b'exit')
        rep = self.ctrl.recv()
        assert rep == b'ok'
        self.ctrl.close()
        self.thread.join()

    def __del__(self):
        self.stop()


class CoreManager:
    def __init__(self, config):
        self.config = {
            'ZMQ_CONTEXT_FACTORY': zmq.Context,
            'GET_USER': self._get_user,
            'SERVER_PUBLIC': SERVER_PUBLIC,
            'TEST_CONTROL_PIPE': 'inproc://core-control.01',
            'CLIENTS_SOCKET_URL': 'tcp://127.0.0.1:0',
            **(config or {})
        }
        self.core = None
        self.ctrl = None
        self.thread = None
        self.addr = None

    def _get_user(self, userid, password):
        try:
            return AVAILABLE_USERS[userid]
        except KeyError:
            return None

    def start(self):
        # TODO: Use subprocess instead of daemon ?
        self.core = CoreApp(self.config)
        self.thread = Thread(target=self.core.run, daemon=True)
        self.thread.start()
        self.ctrl = self.core.zmqcontext.socket(zmq.REQ)
        self.ctrl.connect(self.core.config['TEST_CONTROL_PIPE'])
        self.ctrl.send(b'status')
        rep = self.ctrl.recv()
        assert rep == b'ready'
        # Binded port is dynamically choosen, so extract it from socket
        self.addr = self.core.clients_socket.getsockopt(zmq.LAST_ENDPOINT).decode()

    @contextmanager
    def connected(self, authid=None, authpw=None):
        ctx = zmq.Context()
        socket = ctx.socket(zmq.REQ)
        socket.connect(self.addr)
        cooked_sock = CookedSock(socket)
        if authid:
            assert authid in AVAILABLE_USERS
            cooked_sock.send({'cmd': 'login', 'id': authid, 'password': authpw})
            rep = cooked_sock.recv()
            assert rep == {'status': 'ok'}
        try:
            yield cooked_sock
            if authid:
                cooked_sock.send({'cmd': 'logout'})
                rep = cooked_sock.recv()
                assert rep == {'status': 'ok'}
        finally:
            socket.close()

    def stop(self):
        self.ctrl.send(b'exit')
        rep = self.ctrl.recv()
        assert rep == b'ok'
        self.ctrl.close()
        self.thread.join()

    def __del__(self):
        self.stop()


class BaseBackendTest:

    @classmethod
    def setup_class(cls, config=None, with_users=True):
        cls.backend = BackendManager(config)
        cls.backend.start()
        if with_users:
            for userid, userkeys in AVAILABLE_USERS.items():
                cls.backend.backend.db.pubkey_add(userid, userkeys['public'])
            cls.available_users = AVAILABLE_USERS.copy()
        else:
            cls.available_users = {}

    @classmethod
    def teardown_class(cls):
        cls.backend.stop()


class BaseCoreTest(BaseBackendTest):
    @classmethod
    def setup_class(cls, config=None, backend_config=None, with_users=True):
        super().setup_class(backend_config, with_users=with_users)
        config = config or {}
        config['BACKEND_URL'] = cls.backend.addr
        cls.core = CoreManager(config)
        cls.core.start()

    @classmethod
    def teardown_class(cls):
        cls.core.stop()
        super().teardown_class()
