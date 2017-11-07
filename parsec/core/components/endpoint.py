from parsec.core.components.base import Component, ComponentNames
from parsec.ipc.message import *
import zmq


class EndPoint(Component):
    name = ComponentNames.ENDPOINT

    def init(self):
        self.reply = self.context.socket(zmq.PUSH)
        self.reply.connect('inproc://{}'.format(ComponentNames.REPLY))

    def process(self, msg):
        msg.receiver = ComponentNames.REPLY
        self.reply.send(msg.dumps())
