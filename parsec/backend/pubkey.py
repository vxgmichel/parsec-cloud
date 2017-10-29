from marshmallow import fields

from parsec.tools import UnknownCheckedSchema


class cmd_PUBKEY_GET_Schema(UnknownCheckedSchema):
    id = fields.String(required=True)


def api_pubkey_get(app, req):
    msg = cmd_PUBKEY_GET_Schema().load(req.msg)
    key = app.db.pubkey_get(msg['id'])
    return {'status': 'ok', 'id': msg['id']}, key


# def api_pubkey_add(app, req):
#     msg = cmd_PUBKEY_ADD_Schema().load(msg)
#     key = yield Effect(EPubKeyGet(**msg, raw=True))
#     return {'status': 'ok', 'id': msg['id'], 'key': key}
