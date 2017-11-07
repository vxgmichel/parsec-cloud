from parsec.ipc.message import Message, ExitMessage
from threading import Thread
from sys import stderr
import traceback
import zmq


class Peer(Thread):
    def __init__(self, ctx, name, cb, pushers=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if pushers is None:
            pushers = []

        self.context = ctx
        self.name = name
        self.component = cb
        self.pushers = pushers

    def _bind_sockets(self):
        puller = self.context.socket(zmq.PULL)
        puller.bind('inproc://{0}'.format(self.name))

        pushers = {
            name: self.context.socket(zmq.PUSH)
            for name in self.pushers
        }

        for name in self.pushers:
            pushers[name].connect('inproc://{0}'.format(name))

        return puller, pushers

    def run(self):
        puller, pushers = self._bind_sockets()
        receiving = True

        try:
            msg = self.component.init()

            if msg is not None:
                pusher = pushers.get(msg.receiver)

                if pusher is not None:
                    pusher.send(msg.dumps())

        except Exception:
            print(
                'An unexpected error occured while initializing <<',
                self.name,
                '>>:'
            )
            traceback.print_exc(file=stderr)
            return

        while receiving:
            try:
                raw_msg = puller.recv()
                msg = Message.loads(raw_msg)

                if msg is not None:
                    if isinstance(msg, ExitMessage):
                        receiving = False

                    for new_msg in self.component.process(msg):
                        pusher = pushers.get(new_msg.receiver)

                        if pusher is not None:
                            pusher.send(new_msg.dumps())

            except Exception:
                print('An unexpected error occured in <<', self.name, '>>:')
                traceback.print_exc(file=stderr)

        try:
            msg = self.component.deinit()

            if msg is not None:
                pusher = pushers.get(msg.receiver)

                if pusher is not None:
                    pusher.send(msg.dumps())

        except Exception:
            print(
                'An unexpected error occured while deinitializing <<',
                self.name,
                '>>:'
            )
            traceback.print_exc(file=stderr)


class Network(object):
    def __init__(self, context, components, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.context = context
        self.peers = []

        for cmp in components:
            cmpnames = [
                ocmp.name
                for ocmp in components
                if ocmp is not cmp
            ]
            th = Peer(context, cmp.name, cmp, pushers=cmpnames)
            self.peers.append(th)

    @property
    def entrypoint(self):
        return self.peers[0]

    @property
    def endpoint(self):
        return self.peers[-1]

    def start(self):
        for th in self.peers:
            self.th.start()

    def join(self):
        for th in self.peers:
            self.th.join()
