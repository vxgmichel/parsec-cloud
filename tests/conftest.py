import pytest
import zmq
import json
from threading import Thread

from parsec.backend.app import BackendApp
from parsec.core.app import CoreApp


SERVER_SECRET = b"CzXP)X*eM0<=*fz0#>oWiWEvv?ZMYN+=IzL)rHG>"
SERVER_PUBLIC = b"^NgquxgGVp}vDq[pO<D3[qEt=Py&j@Em69T<[QYa"


# Authentication is only enable through tcp
@pytest.fixture
def backend(socket_addr='tcp://127.0.0.1:0'):
    # TODO: Use subprocess instead of daemon ?
    backend = BackendApp({
        'SERVER_PUBLIC': SERVER_PUBLIC,
        'SERVER_SECRET': SERVER_SECRET,
        'TEST_CONTROL_PIPE': 'inproc://backend-control.01',
        'CMDS_SOCKET_URL': socket_addr,
    })
    thread = Thread(target=backend.run, daemon=True)
    thread.start()
    ctrl = backend.zmqcontext.socket(zmq.REQ)
    ctrl.connect(backend.config['TEST_CONTROL_PIPE'])
    ctrl.send(b'status')
    rep = ctrl.recv()
    assert rep == b'ready'
    yield backend
    ctrl.send(b'exit')
    ctrl.recv()
    ctrl.close()
    thread.join()


@pytest.fixture
def backend_addr(backend):
    # Binded port is dynamically choosen, so extract it from socket
    return backend.cmds_socket.getsockopt(zmq.LAST_ENDPOINT).decode()


@pytest.fixture
def core(backend_addr, client_addr='tcp://127.0.0.1:0'):
    # TODO: Use subprocess instead of daemon ?
    core = CoreApp({
        'SERVER_PUBLIC': SERVER_PUBLIC,
        'TEST_CONTROL_PIPE': 'inproc://core-control.01',
        'CLIENTS_SOCKET_URL': client_addr,
        'BACKEND_URL': backend_addr,
    })
    thread = Thread(target=core.run, daemon=True)
    thread.start()
    ctrl = core.zmqcontext.socket(zmq.REQ)
    ctrl.connect(core.config['TEST_CONTROL_PIPE'])
    ctrl.send(b'status')
    rep = ctrl.recv()
    assert rep == b'ready'
    yield core
    ctrl.send(b'exit')
    ctrl.recv()
    ctrl.close()
    thread.join()


@pytest.fixture
def core_addr(core):
    # Binded port is dynamically choosen, so extract it from socket
    return core.clients_socket.getsockopt(zmq.LAST_ENDPOINT).decode()


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
