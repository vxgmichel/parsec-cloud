import pickle
from hashlib import sha256

from parsec.utils import from_jsonb64
from parsec.core.fs.data import remote_to_local_manifest
from parsec.encryption_manager import decrypt_with_symkey


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
        block = decrypt_with_symkey(access["key"], ciphered)
        # TODO: better exceptions
        assert sha256(block).digest() == access["digest"]

        self.local_db.set(access, block)

    async def load_manifest(self, access):
        rep = await self.backend_cmds_sender.send(
            {"cmd": "vlob_read", "id": access["id"], "rts": access["rts"]}
        )
        # TODO: validate answer
        assert rep["status"] == "ok"
        ciphered = from_jsonb64(rep["blob"])
        raw = self.encryption_manager.decrypt_with_secret_key(access["key"], ciphered)
        remote_manifest = pickle.loads(raw)
        local_manifest = remote_to_local_manifest(remote_manifest)

        self.local_db.set(access, local_manifest)
