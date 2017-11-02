import zmq
import json

from tests.common import BaseCoreTest


class TestControlAPI(BaseCoreTest):
    def test_simple(self):
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.REQ)
        sock.connect(self.core.addr)
        sock.send(b'{"cmd": "status"}')
        raw_rep = sock.recv()
        rep = json.loads(raw_rep.decode())
        assert rep == {"status": "ok"}
