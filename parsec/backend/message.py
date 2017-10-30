import attr
from collections import defaultdict
from marshmallow import fields

from parsec.tools import BaseCmdSchema


class cmd_NEW_Schema(BaseCmdSchema):
    recipient = fields.String(required=True)


class cmd_GET_Schema(BaseCmdSchema):
    offset = fields.Int(validate=lambda n: n >= 0, missing=0)
    limit = fields.Int(validate=lambda n: n > 0, missing=None)


def api_message_new(app, req):
    msg = cmd_NEW_Schema().load(req.msg)
    try:
        bodyframe = req.exframes[0]
    except IndexError:
        return {'status': 'missing_body_frame'}
    recipient = msg['recipient']
    app.db.message_send(recipient, bodyframe)
    return {'status': 'ok'}


def api_message_get(app, req):
    msg = cmd_GET_Schema().load(req.msg)
    messages = app.db.message_get(req.userid, offset=msg['offset'], limit=msg['limit'])
    return ({'status': 'ok', 'offset': msg['offset'], 'count': len(messages)}, *messages)
