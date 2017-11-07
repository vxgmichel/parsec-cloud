from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *


class FileBlockService(Component):
    name = ComponentNames.FILE_BLOCK_SERVICE

    def init(self):
        pass

    def process(self, msg):
        if isinstance(msg, ExitMessage):
            msg.receiver = ComponentNames.SYNCHRONIZER
            yield msg

        elif isinstance(msg, Command):
            resp = Response(
                sender=self.name,
                receiver=ComponentNames.ENDPOINT,
                status='ok'
            )
            yield resp

    def deinit(self):
        pass
