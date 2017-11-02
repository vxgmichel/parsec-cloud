import zmq
import attr

from parsec.core.config import CONFIG
from parsec.core.api import init_control_api
from parsec.exceptions import ParsecError
from parsec.tools import ejson_loads, ejson_dumps


@attr.s(slots=True)
class RequestContext:
    reqid = attr.ib()
    msg = attr.ib()
    exframes = attr.ib()


def _build_request(frames):
    if len(frames) < 3:
        return None
    reqid, _, msgframe, *exframes = frames
    try:
        msg = ejson_loads(msgframe.bytes.decode())
    except Exception:
        # Invalid msg
        return None
    return RequestContext(
        reqid,
        msg,
        exframes
    )


def _build_response(reqid, rep):
    if isinstance(rep, (list, tuple)):
        repmsg, *exframes = rep
    else:
        repmsg = rep
        exframes = ()
    return (reqid, b'', ejson_dumps(repmsg).encode(), *exframes)


class CoreApp:
    def __init__(self, config=None):
        self.config = CONFIG.copy()
        self.zmqcontext = None
        self.clients_socket = None
        self.cmds = {}
        if config:
            self.config.update(config)
        self._bootstrap()

    def _bootstrap(self):
        init_control_api(self)

        # Configure auth
        self.zmqcontext = zmq.Context.instance()
        # Client socket listen on localhost, no need for authentication
        self.clients_socket = self.zmqcontext.socket(zmq.ROUTER)
        self._get_user = self.config.get('GET_USER', self._get_user_from_confpath)

    def _get_user_from_confpath(self, userid, password):
        # TODO
        raise NotImplementedError()

    def run(self):
        self.clients_socket.bind(self.config['CLIENTS_SOCKET_URL'])

        poller = zmq.Poller()
        poller.register(self.clients_socket, zmq.POLLIN)

        if self.config['TEST_CONTROL_PIPE']:
            control_pipe = self.zmqcontext.socket(zmq.REP)
            control_pipe.bind(self.config['TEST_CONTROL_PIPE'])
            poller.register(control_pipe, zmq.POLLIN)

        try:
            exiting = False
            while not exiting:
                for sock, _ in poller.poll():
                    if sock is self.clients_socket:
                        frames = self.clients_socket.recv_multipart(copy=False)
                        repframes = self._handle_cmd(frames)
                        self.clients_socket.send_multipart(repframes)
                    else:  # Control pipe for testing
                        cmd = control_pipe.recv()
                        if cmd == b'exit':
                            control_pipe.send(b'ok')
                            exiting = True
                        elif cmd == b'status':
                            control_pipe.send(b'ready')
                        else:
                            control_pipe.send(b'unknown cmd')
        except KeyError:
            pass
        finally:
            for sock, _ in poller.sockets:
                sock.close()

    def _handle_cmd(self, frames):
        print('REQ: ', [f.bytes for f in frames])
        req = _build_request(frames)
        if not req:
            repframes = (frames[0], b'', b'{"status": "bad_msg"}')
        else:
            try:
                cmd = self.cmds[req.msg['cmd']]
            except KeyError:
                repframes = _build_response(req.reqid, {"status": "unknown_cmd"})
            else:
                try:
                    rep = cmd(self, req)
                except ParsecError as exc:
                    rep = exc.to_dict()
                repframes = _build_response(req.reqid, rep)
        print('REP: ', repframes)
        return repframes

    def register_cmd(self, cmd, func):
        assert cmd not in self.cmds
        self.cmds[cmd] = func
