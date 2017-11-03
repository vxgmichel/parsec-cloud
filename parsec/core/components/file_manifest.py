from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *
from queue import Queue


class FileManifest(Component):
    name = ComponentNames.FILE_MANIFEST

    def init(self):
        pass

    def process(self, msg):
        if isinstance(msg, ExitMessage):
            msg.receiver = ComponentNames.BLOCK
            yield msg

        elif isinstance(msg, ReadFileCommand):
            yield ReadFileManifest(
                sender=self.name,
                receiver=ComponentNames.BACKEND,
                id=msg.info['id']
            )

        elif isinstance(msg, Response):
            if msg.status != Response.Status.OK:
                msg.receiver = msg.sender
                msg.sender = self.name
                yield msg

            else:
                yield ReadBlockCommand(
                    sender=self.name,
                    receiver=msg.sender,

                )
