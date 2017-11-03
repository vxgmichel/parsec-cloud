from enum import IntEnum, auto
import attr


@attr.s(slots=True)
class Message(object):
    sender = attr.ib()
    receiver = attr.ib()


@attr.s(slots=True)
class ExitMessage(Message):
    pass


@attr.s(slots=True)
class Response(Message):
    status = attr.ib()
    content = attr.ib(default=None)

    class Status(IntEnum):
        OK = auto()
        UNKNOWN_COMMAND = auto()
        NOT_FOUND = auto()
        UNKNOWN_PATH = auto()
        NOT_A_FILE = auto()


@attr.s(slots=True)
class ReadUserManifest(Message):
    pass


@attr.s(slots=True)
class ReadFileManifest(Message):
    id = attr.ib()


@attr.s(slots=True)
class Command(Message):
    path = attr.ib()


@attr.s(slots=True)
class StatCommand(Command):
    pass


@attr.s(slots=True)
class ReadFileCommand(Command):
    info = attr.ib(default=None)


@attr.s(slots=True)
class WriteFileCommand(Command):
    pass


@attr.s(slots=True)
class CreateFileCommand(Command):
    pass


@attr.s(slots=True)
class DeleteCommand(Command):
    pass
