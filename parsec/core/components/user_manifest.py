from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *
from queue import Queue


class UserManifest(Component):
    name = ComponentNames.USER_MANIFEST

    def init(self):
        self.pending = Queue()
        self.user_manifest = None

        return ReadUserManifest(
            sender=self.name,
            receiver=ComponentNames.BACKEND
        )

    def process_StatCommand(self, msg):
        if msg.path in self.user_manifest:
            return Response(
                sender=self.name,
                receiver=msg.sender,
                status=Response.Status.OK,
                content={
                    'type': self.user_manifest[msg.path]['type']
                }
            )

        else:
            return Response(
                sender=self.name,
                receiver=msg.sender,
                status=Response.Status.UNKNOWN_PATH
            )

    def process_ReadFileCommand(self, msg):
        if msg.path in self.user_manifest:
            file_info = self.user_manifest[msg.path]

            if file_info['type'] == 'folder':
                return Response(
                    sender=self.name,
                    receiver=msg.sender,
                    status=Response.Status.NOT_A_FILE
                )

            else:
                return ReadFileCommand(
                    sender=msg.sender,
                    receiver=ComponentNames.FILE_MANIFEST,
                    path=msg.path,
                    info=file_info
                )

        else:
            return Response(
                sender=self.name,
                receiver=msg.sender,
                status=Response.Status.UNKNOWN_PATH
            )

    def process_Unknown(self, msg):
        return Response(
            sender=self.name,
            receiver=msg.sender,
            status=Response.Status.UNKNOWN_COMMAND
        )

    def process(self, msg):
        if isinstance(msg, ExitMessage):
            msg.receiver = ComponentNames.FILE_MANIFEST
            yield msg

        elif isinstance(msg, Response):
            if msg.status != Response.Status.OK:
                raise Component.Error("Non-ok response from backend")

            self.user_manifest = msg.content

            while not self.pending.empty():
                msg = self.pending.get()
                processor_name = 'process_{0}'.format(msg.__class__.__name__)
                processor = getattr(self, processor_name, self.process_Unknown)
                yield processor(msg)

        elif self.user_manifest is None:
            self.pending.put(msg)

        else:
            processor_name = 'do_{0}'.format(msg.__class__.__name__)
            processor = getattr(self, processor_name, self.process_Unknown)
            yield processor(msg)

    def deinit(self):
        pass
