from enum import IntEnum, auto
import pickle
import attr


@attr.s(slots=True)
class Message(object):
    sender = attr.ib()
    receiver = attr.ib()

    def dumps(self):
        return pickle.dumps(self)

    @staticmethod
    def loads(raw):
        return pickle.loads(raw)


@attr.s(slots=True)
class ExitMessage(Message):
    pass


@attr.s(slots=True)
class Command(Message):
    type = attr.ib()
    payload = attr.ib()


class CommandType(IntEnum):
    FILE_CREATE = auto()
    FILE_READ = auto()
    FILE_WRITE = auto()
    STAT = auto()
    FOLDER_CREATE = auto()
    MOVE = auto()
    DELETE = auto()
    FILE_TRUNCATE = auto()


@attr.s(slots=True)
class Response(Message):
    status = attr.ib()
