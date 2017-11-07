from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *


class Synchronizer(Component):
    name = ComponentNames.SYNCHRONIZER

    def init(self):
        pass

    def process(self, msg):
        if isinstance(msg, ExitMessage):
            msg.receiver = ComponentNames.ENDPOINT
            yield msg

    def deinit(self):
        pass
