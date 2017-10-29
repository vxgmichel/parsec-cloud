import attr
from collections import defaultdict
from marshmallow import fields

from parsec.tools import UnknownCheckedSchema


class cmd_NEW_Schema(UnknownCheckedSchema):
    recipient = fields.String(required=True)
    body = fields.Base64Bytes(required=True)


class cmd_GET_Schema(UnknownCheckedSchema):
    offset = fields.Int(missing=0)
    length = fields.Int(missing=None)


def api_message_new(app, req):
    msg = cmd_NEW_Schema().load(req.msg)
    try:
        body = req.frames[2]
    except IndexError:
        return {'status': 'missing_body_frame'}
    recipient = msg['recipient']
    app.db.message_send(recipient, body)
    return {'status': 'ok'}


def api_message_get(app, req):
    msg = cmd_GET_Schema().load(req.msg)
    messages = app.db.message_get(req.userid, offset=msg['offset'], length=msg['offset'])
    return ({'status': 'ok'}, *messages)
