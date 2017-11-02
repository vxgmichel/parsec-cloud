import pytest
import zmq
import json
from threading import Thread

from parsec.backend.app import BackendApp
from parsec.core.app import CoreApp

from tests.common import CookedSock, SERVER_SECRET, SERVER_PUBLIC


# Authentication is only enable through tcp
@pytest.fixture
def backend(socket_addr='tcp://127.0.0.1:0'):
    # TODO: Use subprocess instead of daemon ?
    backend = BackendApp({
        'SERVER_PUBLIC': SERVER_PUBLIC,
        'SERVER_SECRET': SERVER_SECRET,
        'TEST_CONTROL_PIPE': 'inproc://backend-control.01',
        'CMDS_SOCKET_URL': socket_addr,
        'BLOCKSTORE_URL': '<inbackend>',
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
