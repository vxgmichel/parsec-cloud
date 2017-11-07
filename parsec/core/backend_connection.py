import zmq
import zmq.devices

from parsec.tools import b64_to_z85


class BackendConnection:
    def __init__(self, app):
        self.backend_sock = None
        assert app.config.get('BACKEND_URL')
        app.config.setdefault('BACKEND_CONNECTION_ADDR', 'inproc://backend-connection.01')
        self.app = app

    def start(self):
        # Multiplexing backend connection between pipeline stages
        self.backend_sock = zmq.devices.ThreadDevice(zmq.QUEUE, zmq.REP, zmq.REQ)
        self.backend_sock.context_factory = lambda: self.app.zmqcontext

        self.backend_sock.bind_in(self.app.config.get('BACKEND_CONNECTION_ADDR'))

        _, userkeys = self.app.extensions['logged_user']
        self.backend_sock.setsockopt_out(
            zmq.CURVE_SERVERKEY, b64_to_z85(self.app.config['SERVER_PUBLIC']))
        self.backend_sock.setsockopt_out(
            zmq.CURVE_SECRETKEY, b64_to_z85(userkeys['private']))
        self.backend_sock.setsockopt_out(
            zmq.CURVE_PUBLICKEY, b64_to_z85(userkeys['public']))
        self.backend_sock.connect_out(self.app.config['BACKEND_URL'])
        self.backend_sock.setsockopt_out(zmq.LINGER, 0)

        self.backend_sock.start()

    def stop(self):
        # Note the zmq device will be automatically killed once the
        # zmq context gets terminated, so nothing to do here
        pass

    def ping(self):
        try:
            sock = self.app.zmqcontext.socket(zmq.REQ)
            sock.connect(self.app.config['BACKEND_CONNECTION_ADDR'])
            sock.send_json({"cmd": "ping"})
            rep = sock.recv_json()
            return rep == {'status': 'ok'}
        except Exception:
            return False
