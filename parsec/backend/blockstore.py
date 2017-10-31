from marshmallow import fields
from uuid import uuid4

from parsec.tools import BaseCmdSchema


def generate_block_id():
    return uuid4().hex


class cmd_Schema(BaseCmdSchema):
    id = fields.String(
        validate=lambda n: 0 < len(n) <= 32,
        missing=lambda: generate_block_id()  # lambda for easier mocking
    )


def api_blockstore_get_url(app, req):
    return {'status': 'ok', 'url': app.config['BLOCKSTORE_URL']}


class InMemoryBlockStore:
    def __init__(self):
        self.blocks = {}

    def api_blockstore_post(self, app, req):
        msg = cmd_Schema().load(req.msg)
        try:
            bodyframe = req.exframes[0]
        except IndexError:
            return {'status': 'missing_body_frame'}
        if msg['id'] in self.blocks:
            return {'status': 'block_id_already_exists'}
        self.blocks[msg['id']] = bodyframe.bytes
        return {'status': 'ok'}


    def api_blockstore_get(self, app, req):
        msg = cmd_Schema().load(req.msg)
        try:
            block = self.blocks[msg['id']]
        except KeyError:
            return {'status': 'block_not_found'}
        return {'status': 'ok'}, block
