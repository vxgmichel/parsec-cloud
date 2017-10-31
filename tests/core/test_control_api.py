import zmq
import json


def test_simple(core_addr):
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.connect(core_addr)
    sock.send(b'{"cmd": "status"}')
    raw_rep = sock.recv()
    rep = json.loads(raw_rep.decode())
    assert rep == {"status": "ok"}
