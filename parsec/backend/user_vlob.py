from marshmallow import fields

from parsec.tools import BaseCmdSchema


class cmd_READ_Schema(BaseCmdSchema):
    version = fields.Int(validate=lambda n: n >= 0, missing=None)


class cmd_UPDATE_Schema(BaseCmdSchema):
    version = fields.Int(validate=lambda n: n > 0)


def api_user_vlob_read(app, req):
    msg = cmd_READ_Schema().load(req.msg)
    version = msg['version']
    blob, version = app.db.user_vlob_read(req.userid, version)
    return ({
        'status': 'ok',
        'version': version
    }, blob)


def api_user_vlob_update(app, req):
    msg = cmd_UPDATE_Schema().load(req.msg)
    try:
        bodyframe = req.exframes[0]
    except IndexError:
        return {'status': 'missing_body_frame'}
    app.db.user_vlob_update(req.userid, msg['version'], bodyframe)
    return {'status': 'ok'}
