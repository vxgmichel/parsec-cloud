import pickle

from parsec.utils import from_jsonb64

try:
    from parsec.core.fs.utils import verify, sym_decrypt
except ImportError:

    def verify(content):
        return "alice@test", content

    def sym_decrypt(key, content):
        return content


from parsec.core.fs.data import remote_to_local_manifest


class RemoteLoader:
    def __init__(self, backend_cmds_sender, encryption_manager, local_db):
        self.backend_cmds_sender = backend_cmds_sender
        self.encryption_manager = encryption_manager
        self.local_db = local_db

    async def load_block(self, access):
        rep = await self.backend_cmds_sender.send({"cmd": "blockstore_read", "id": access["id"]})
        # TODO: validate answer
        assert rep["status"] == "ok"
        ciphered = from_jsonb64(rep["block"])
        block = sym_decrypt(access["key"], ciphered)
        # TODO: check block hash
        self.local_db.set(access, block)

    async def load_manifest(self, access):
        rep = await self.backend_cmds_sender.send(
            {"cmd": "vlob_read", "id": access["id"], "rts": access["rts"]}
        )
        # TODO: validate answer
        assert rep["status"] == "ok"
        ciphered = from_jsonb64(rep["blob"])
        signed = sym_decrypt(access["key"], ciphered)

        author, raw = verify(sym_decrypt(access["key"], signed))
        remote_manifest = pickle.loads(raw)
        local_manifest = remote_to_local_manifest(remote_manifest)

        self.local_db.set(access, local_manifest)
