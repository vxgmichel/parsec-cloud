import pytest
import zmq
import json
from threading import Thread
from contextlib import contextmanager

from parsec.backend.app import BackendApp
from parsec.core.app import CoreApp


SERVER_SECRET = b"CzXP)X*eM0<=*fz0#>oWiWEvv?ZMYN+=IzL)rHG>"
SERVER_PUBLIC = b"^NgquxgGVp}vDq[pO<D3[qEt=Py&j@Em69T<[QYa"


AVAILABLE_USERS = {
    # userid: (public_key, private_key)
    'alice@test.com': (b"ve>exeUXhI:mNY%TAQ9>}?/%/=52fw[Kd$z6U{N<", b"mttsOg+W3=[TQ94Le[eRc3V73!@7r)fs7gafQG[P"),
    'bob@test.com': (b"J3{a{k[}-9@Mo3$taFD.slOx?Wqt@H7<Xt:ygMmf", b"3eju8rs-E1^j!Y)yx<X5*LZ)w!b:waZ:XF9LV4nf"),
}


@pytest.fixture
def alice(backend):
    alice_public = zmq.utils.z85.decode(b"ve>exeUXhI:mNY%TAQ9>}?/%/=52fw[Kd$z6U{N<")
    alice_private = zmq.utils.z85.decode(b"mttsOg+W3=[TQ94Le[eRc3V73!@7r)fs7gafQG[P")
    alice_id = 'alice@test.com'
    # Who cares about concurrency anyway ?
    backend.db._pubkeys[alice_id] = alice_public
    return {
        'id': alice_id,
        'private': alice_private,
        'public': alice_public
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


@pytest.fixture
def alicesock(backend_addr, alice):
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = zmq.utils.z85.encode(alice['private'])
    socket.curve_publickey = zmq.utils.z85.encode(alice['public'])
    socket.curve_serverkey = SERVER_PUBLIC
    socket.connect(backend_addr)
    yield CookedSock(socket)
    socket.close()


@pytest.fixture
def bob(backend):
    bob_public = zmq.utils.z85.decode(b"J3{a{k[}-9@Mo3$taFD.slOx?Wqt@H7<Xt:ygMmf")
    bob_private = zmq.utils.z85.decode(b"3eju8rs-E1^j!Y)yx<X5*LZ)w!b:waZ:XF9LV4nf")
    bob_id = 'bob@test.com'
    # Who cares about concurrency anyway ?
    backend.db._pubkeys[bob_id] = bob_public
    return {
        'id': bob_id,
        'private': bob_private,
        'public': bob_public
    }


@pytest.fixture
def bobsock(backend_addr, bob):
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = zmq.utils.z85.encode(bob['private'])
    socket.curve_publickey = zmq.utils.z85.encode(bob['public'])
    socket.curve_serverkey = SERVER_PUBLIC
    socket.connect(backend_addr)
    yield CookedSock(socket)
    socket.close()


class BackendManager:
    def __init__(self, config):
        self.config = {
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
    def connected_as(self, userid):
        userpublic, userprivate = AVAILABLE_USERS[userid]
        ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.REQ)
        socket.curve_secretkey = userprivate
        socket.curve_publickey = userpublic
        socket.curve_serverkey = SERVER_PUBLIC
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
    def connected_as(self, userid):
        assert userid in AVAILABLE_USERS
        ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.REQ)
        socket.connect(self.addr)
        cooked_sock = CookedSock(socket)
        cooked_sock.send({'cmd': 'login', 'id': userid, 'password': ''})  # TODO
        rep = cooked_sock.recv()
        assert rep == {'status': 'ok'}
        yield cooked_sock
        cooked_sock.send({'cmd': 'logout'})
        rep = cooked_sock.recv()
        assert rep == {'status': 'ok'}
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

    def setup(self, config=None, with_users=True):
        import pdb; pdb.set_trace()
        self.backend = BackendManager(config)
        self.backend.start()
        if with_users:
            for userid, userkeys in AVAILABLE_USERS.items():
                self.backend.backend.db.pubkey_add(
                    userid, zmq.utils.z85.decode(userkeys[0]))

    def teardown(self):
        self.backend.stop()


class BaseCoreTest(BaseBackendTest):
    def setup(self, config=None, backend_config=None, with_users=True):
        super().setup(backend_config, with_users=with_users)
        self.core = CoreManager(config)
        self.core.start()

    def teardown(self):
        self.core.stop()
        super().teardown()
