import zmq
from os import environ


CONFIG = {
    'ZMQ_CONTEXT_FACTORY': zmq.Context.instance,
    'SERVER_PUBLIC': '',
    'SERVER_SECRET': '',
    'TEST_CONTROL_PIPE': '',
    'DB_URL': environ.get('DB_URL', '<inmemory>'),
    'BLOCKSTORE_URL': environ.get('BLOCKSTORE_URL', '<inbackend>'),
}
