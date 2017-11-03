import zmq
from os import environ


CONFIG = {
    'ZMQ_CONTEXT_FACTORY': zmq.Context.instance,
    'SERVER_PUBLIC': '',
    'CLIENTS_SOCKET_URL': environ.get('CLIENTS_SOCKET_URL', 'tcp://localhost:9090'),
    'BACKEND_URL': environ.get('BACKEND_URL', '')
}
