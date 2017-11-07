import zmq
import attr

from parsec.backend.config import CONFIG
from parsec.backend.api import init_api
from parsec.backend.auth import authenticator_factory
from parsec.exceptions import ParsecError
from parsec.tools import ejson_loads, ejson_dumps, b64_to_z85


@attr.s(slots=True)
class RequestContext:
    reqid = attr.ib()
    userid = attr.ib()
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
        msgframe['User-Id'],
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


class BackendApp:
    def __init__(self, config=None):
        self.config = CONFIG.copy()
        self.db = None
        self.zmqcontext = None
        self.authenticator = None
        self.cmds_socket = None
        self.extensions = {}
        self.cmds = {}
        if config:
            self.config.update(config)
        self._bootstrap()

    def _bootstrap(self):
        db_url = self.config['DB_URL']
        if db_url.startswith('postgres://'):
            from parsec.backend.db.postgresql import PostgreSQLDB
            self.db = PostgreSQLDB(self)
        elif db_url == '<inmemory>':
            from parsec.backend.db.inmemory import InMemoryDB
            self.db = InMemoryDB(self)
        else:
            raise RuntimeError('Unknown db type `%s`' % db_url)
        init_api(self)

        # Configure auth
        self.zmqcontext = self.config['ZMQ_CONTEXT_FACTORY']()
        self.authenticator = authenticator_factory(self)
        # self.authenticator.configure_curve(domain='*', location=public_keys_dir)

        self.cmds_socket = self.zmqcontext.socket(zmq.ROUTER)
        self.cmds_socket.curve_secretkey = b64_to_z85(self.config['SERVER_SECRET'])
        self.cmds_socket.curve_publickey = b64_to_z85(self.config['SERVER_PUBLIC'])
        self.cmds_socket.curve_server = True  # must come before bind

    def _teardown(self):
        self.zmqcontext.term()

    def __del__(self):
        self._teardown()

    def run(self):
        self.authenticator.start()
        # TODO: must make sure authenticator is running before binding the cmds socket
        self.cmds_socket.bind(self.config['CMDS_SOCKET_URL'])

        poller = zmq.Poller()
        poller.register(self.cmds_socket, zmq.POLLIN)

        if self.config['TEST_CONTROL_PIPE']:
            control_pipe = self.zmqcontext.socket(zmq.REP)
            control_pipe.bind(self.config['TEST_CONTROL_PIPE'])
            poller.register(control_pipe, zmq.POLLIN)

        try:
            exiting = False
            while not exiting:
                for sock, _ in poller.poll():
                    if sock is self.cmds_socket:
                        frames = self.cmds_socket.recv_multipart(copy=False)
                        repframes = self._handle_cmd(frames)
                        self.cmds_socket.send_multipart(repframes)
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
            self.authenticator.stop()

    def _anonymous_cmd(self, app, req):
        if req.msg['cmd'] == 'ping':
            return {"status": "ok"}
        else:
            return {"status": "unknown_cmd"}

    def _handle_cmd(self, frames):
        print('REQ: ', [f.bytes for f in frames])
        req = _build_request(frames)
        if not req:
            repframes = (frames[0], b'', b'{"status": "bad_msg"}')
        else:
            try:
                if req.userid == '<Anonymous>':
                    # Anonymous connexion !
                    cmd = self._anonymous_cmd
                else:
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
