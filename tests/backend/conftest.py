import pytest
import zmq
import json
from threading import Thread

from parsec.backend.app import BackendApp
from parsec.core.app import CoreApp
from parsec.tools import b64_to_z85

from tests.common import CookedSock, SERVER_SECRET, SERVER_PUBLIC, AVAILABLE_USERS


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
    backend._teardown()


@pytest.fixture
def backend_addr(backend):
    # Binded port is dynamically choosen, so extract it from socket
    return backend.cmds_socket.getsockopt(zmq.LAST_ENDPOINT).decode()


@pytest.fixture
def alice(backend):
    alice_id = 'alice@test.com'
    # Who cares about concurrency anyway ?
    alice_private, alice_public = AVAILABLE_USERS[alice_id]
    backend.db.pubkey_add(alice_id, alice_public)
    return {
        'id': alice_id,
        'private': alice_private,
        'public': alice_public
    }


@pytest.fixture
def alicesock(backend_addr, alice):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = b64_to_z85(alice['private'])
    socket.curve_publickey = b64_to_z85(alice['public'])
    socket.curve_serverkey = b64_to_z85(SERVER_PUBLIC)
    socket.connect(backend_addr)
    yield CookedSock(socket)
    socket.close()


@pytest.fixture
def bob(backend):
    bob_id = 'bob@test.com'
    # Who cares about concurrency anyway ?
    bob_private, bob_public = AVAILABLE_USERS[bob_id]
    backend.db.pubkey_add(bob_id, bob_public)
    return {
        'id': bob_id,
        'private': bob_private,
        'public': bob_public
    }


@pytest.fixture
def bobsock(backend_addr, bob):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.REQ)
    socket.curve_secretkey = b64_to_z85(bob['private'])
    socket.curve_publickey = b64_to_z85(bob['public'])
    socket.curve_serverkey = b64_to_z85(SERVER_PUBLIC)
    socket.connect(backend_addr)
    yield CookedSock(socket)
    socket.close()
