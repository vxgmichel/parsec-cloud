import asyncio
import inspect
import sys
import base64

from marshmallow import Schema, fields, validates_schema, ValidationError
from logbook import Logger, StreamHandler

from parsec.exceptions import BadMessageError


# TODO: useful ?
LOG_FORMAT = '[{record.time:%Y-%m-%d %H:%M:%S.%f%z}] ({record.thread_name})' \
             ' {record.level_name}: {record.channel}: {record.message}'
logger = Logger('Parsec')
logger_stream = StreamHandler(sys.stdout, format_string=LOG_FORMAT)
logger_stream.push_application()


def to_jsonb64(raw: bytes):
    return base64.encodebytes(raw).decode()


def from_jsonb64(msg: str):
    return base64.decodebytes(msg.encode())


def async_callback(callback, *args, **kwargs):
    def event_handler(sender):
        loop = asyncio.get_event_loop()
        loop.call_soon(asyncio.ensure_future, callback(sender, *args, **kwargs))
    return event_handler


def event_handler(callback, sender, **kwargs):
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.ensure_future, callback(**kwargs))


def get_arg(method, arg, args, kwargs):
    try:
        return kwargs[arg]
    except KeyError:
        properties = inspect.getargspec(method)
        arg_index = properties.args.index(arg)
        try:
            return args[arg_index]
        except IndexError:
            defaults_values = dict(zip(reversed(properties.args),
                                       reversed(properties.defaults)))
            return defaults_values[arg]


class UnknownCheckedSchema(Schema):

    """
    ModelSchema with check for unknown field
    """

    @validates_schema(pass_original=True)
    def check_unknown_fields(self, data, original_data):
        for key in original_data:
            if key not in self.fields or self.fields[key].dump_only:
                raise ValidationError('Unknown field name {}'.format(key))


class BaseCmdSchema(UnknownCheckedSchema):
    cmd = fields.String(required=True)

    def load(self, msg):
        parsed_msg, errors = super().load(msg)
        if errors:
            raise BadMessageError(errors)
        return parsed_msg
