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
        self.backend_sock.bind_in(self.app.config.get('BACKEND_CONNECTION_ADDR'))
        self.backend_sock.curve_serverkey = b64_to_z85(self.app.config['SERVER_PUBLIC'])

        _, pubkey, privkey = self.app.extensions['logged_user']
        self.backend_sock.curve_secretkey = b64_to_z85(privkey)
        self.backend_sock.curve_publickey = b64_to_z85(pubkey)
        self.backend_sock.connect_out(self.app.config['BACKEND_URL'])
        # socket.setsockopt(zmq.LINGER, 0)
        self.backend_sock.start()
        # TODO: check connection is ok ?

    def stop(self):
        self.backend_sock.stop()
        self.backend_sock = None

    def __del__(self):
        if self.backend_sock:
            self.stop()
