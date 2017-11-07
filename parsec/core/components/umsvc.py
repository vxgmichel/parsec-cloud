from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *


class UserManifestService(Component):
    name = ComponentNames.USER_MANIFEST_SERVICE

    def init(self):
        self.user_manifest = None

    def process(self, msg):
        if isinstance(msg, ExitMessage):
            msg.receiver = ComponentNames.FILE_BLOCK_SERVICE
            yield msg

        elif isinstance(msg, Command):
            if msg.type == CommandType.STAT:
                resp = Response(
                    sender=self.name,
                    receiver=ComponentNames.ENDPOINT,
                    status='ok'
                )
                yield resp

            else:
                msg.receiver = ComponentNames.FILE_BLOCK_SERVICE
                msg.payload.msg['user_manifest'] = self.user_manifest
                yield msg

    def deinit(self):
        pass
