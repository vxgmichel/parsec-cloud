import pytest
import zmq
import json
from threading import Thread

from parsec.backend.app import BackendApp


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
        'CMDS_SOCKET_URL': socket_addr
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
def alice(backend):
    alice_public = b"ve>exeUXhI:mNY%TAQ9>}?/%/=52fw[Kd$z6U{N<"
    alice_private = b"mttsOg+W3=[TQ94Le[eRc3V73!@7r)fs7gafQG[P"
    # Who cares about concurrency anyway ?
    backend.db._pubkeys['alice'] = zmq.utils.z85.decode(alice_public)
    return alice_private, alice_public


class TalkToBackendSock:
    def __init__(self, socket):
        self.socket = socket

    def send(self, msg, *frames):
        self.socket.send_multipart((json.dumps(msg).encode(), *frames))

    def recv(self):
        repframes = self.socket.recv_multipart()
        rep = json.loads(repframes[0])
        return (rep, b''.join(repframes[1:]))


@pytest.fixture
def alicesock(backend_addr, alice):
    alice_private, alice_public = alice
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = alice_private
    socket.curve_publickey = alice_public
    socket.curve_serverkey = SERVER_PUBLIC
    socket.connect(backend_addr)
    yield TalkToBackendSock(socket)
    socket.close()


@pytest.fixture
def bob(backend):
    bob_public = b"J3{a{k[}-9@Mo3$taFD.slOx?Wqt@H7<Xt:ygMmf"
    bob_private = b"3eju8rs-E1^j!Y)yx<X5*LZ)w!b:waZ:XF9LV4nf"
    # Who cares about concurrency anyway ?
    backend.db._pubkeys['bob'] = zmq.utils.z85.decode(bob_public)
    return bob_private, bob_public


@pytest.fixture
def bobsock(backend_addr, bob):
    bob_private, bob_public = bob
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = bob_private
    socket.curve_publickey = bob_public
    socket.curve_serverkey = SERVER_PUBLIC
    socket.connect(backend_addr)
    yield TalkToBackendSock(socket)
    socket.close()
