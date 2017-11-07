from parsec.core.components.base import ComponentNames
from parsec.ipc.p2p import Network
from parsec.ipc.message import *

from parsec.core.components import (
    UserManifestService,
    FileBlockService,
    Synchronizer,
    EndPoint
)


class FSPipeline(object):
    def __init__(self, app, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.context = app.zmqcontext

        self.p2p = Network(
            self.context,
            [
                UserManifestService(),
                FileBlockService(),
                Synchronizer(),
                EndPoint()
            ]
        )

        self.entrypoint = self.context.socket(zmq.PUSH)
        self.endpoint = self.context.socket(zmq.PULL)

    def start(self):
        self.endpoint.bind('inproc://{}'.format(ComponentNames.REPLY))
        self.p2p.start()
        self.entrypoint.connect('inproc://{}'.format(self.p2p.entrypoint.name))

    def stop(self):
        msg = ExitMessage(
            sender='fspipeline',
            receiver=self.p2p.entrypoint.name
        )
        self.entrypoint.send(str(msg))
        self.endpoint.recv()  # ack
        self.p2p.stop()

    def _cmd_sync(self, type, app, req):
        msg = Command(
            sender='fspipeline',
            receiver=self.p2p.entrypoint.name,
            type=type,
            payload=req
        )
        self.entrypoint.send(str(msg))
        response = self.endpoint.recv()

        return {'status': response.status}

    def _cmd_async(self, type, app, req):
        msg = Command(
            sender='fspipeline',
            receiver=self.p2p.entrypoint.name,
            type=type,
            payload=req
        )
        self.entrypoint.send(str(msg))

        return {'status': 'ok'}

    def _cmd_FILE_CREATE(self, app, req):
        return self._cmd_async(CommandType.FILE_CREATE, app, req)

    def _cmd_FILE_READ(self, app, req):
        return self._cmd_sync(CommandType.FILE_READ, app, req)

    def _cmd_FILE_WRITE(self, app, req):
        return self._cmd_async(CommandType.FILE_WRITE, app, req)

    def _cmd_STAT(self, app, req):
        return self._cmd_sync(CommandType.STAT, app, req)

    def _cmd_FOLDER_CREATE(self, app, req):
        return self._cmd_sync(CommandType.FOLDER_CREATE, app, req)

    def _cmd_MOVE(self, app, req):
        return self._cmd_sync(CommandType.MOVE, app, req)

    def _cmd_DELETE(self, app, req):
        return self._cmd_sync(CommandType.DELETE, app, req)

    def _cmd_FILE_TRUNCATE(self, app, req):
        return self._cmd_sync(CommandType.FILE_TRUNCATE, app, req)
