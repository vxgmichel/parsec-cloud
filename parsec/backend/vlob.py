import attr
import random
import string
from uuid import uuid4
from marshmallow import fields

from parsec.tools import UnknownCheckedSchema


TRUST_SEED_LENGTH = 12


def generate_trust_seed():
    # Use SystemRandom to get cryptographically secure seeds
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                   for _ in range(TRUST_SEED_LENGTH))


class cmd_CREATE_Schema(UnknownCheckedSchema):
    id = fields.String(missing=lambda: uuid4().hex, validate=lambda n: 0 < len(n) <= 32)


class cmd_READ_Schema(UnknownCheckedSchema):
    id = fields.String(required=True)
    version = fields.Int(validate=lambda n: n >= 1)
    trust_seed = fields.String(required=True)


class cmd_UPDATE_Schema(UnknownCheckedSchema):
    id = fields.String(required=True)
    version = fields.Int(validate=lambda n: n > 1)
    trust_seed = fields.String(required=True)
    blob = fields.Base64Bytes(required=True)


def api_vlob_create(app, req):
    msg = cmd_CREATE_Schema().load(req.msg)
    try:
        body = req.frames[2]
    except IndexError:
        return {'status': 'missing_body_frame'}
    id = msg['id']
    rts = generate_trust_seed()
    wts = generate_trust_seed()
    app.db.vlob_create(id, rts, wts, body)
    return {
        'status': 'ok',
        'id': id,
        'read_trust_seed': rts,
        'write_trust_seed': wts
    }


def api_vlob_read(app, req):
    msg = cmd_READ_Schema().load(req.msg)
    id, version = msg['id'], msg['version']
    blob = app.db.vlob_read(id, msg['trust_seed'], version)
    return ({
        'status': 'ok',
        'id': id,
        'version': version
    }, blob)


def api_vlob_update(app, req):
    msg = cmd_UPDATE_Schema().load(req.msg)
    try:
        body = req.frames[2]
    except IndexError:
        return {'status': 'missing_body_frame'}
    app.db.vlob_update(id, msg['trust_seed'], msg['version'], body)
    return {'status': 'ok'}
