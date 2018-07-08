from parsec.schema import BaseCmdSchema, fields


class cmd_READ_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    from_index = fields.Integer(required=True)


class BaseBeaconComponent:
    async def api_beacon_read(self, client_ctx, msg):
        msg = cmd_READ_Schema().load_or_abort(msg)
        elems = await self.read(**msg)
        return {"status": "ok", "id": msg["id"], "data": elems, "data_count": len(elems)}

    async def read(self, id, from_index):
        raise NotImplementedError()

    async def update(self, id, data, author="anonymous"):
        raise NotImplementedError()
