import pickle

from parsec.core.fs.utils import verify, sym_decrypt
from parsec.core.fs.data import remote_to_local_manifest


class RemoteLoader:
    def __init__(self, backend_conn, local_db):
        self._backend_conn = backend_conn
        self._local_db = local_db

    async def load_block(self, access):
        ciphered_block = await self._backend_conn.block_read(access["id"])
        block = sym_decrypt(access["key"], ciphered_block)
        # TODO: check block hash
        self._local_db.set(access, block)

    async def load_manifest(self, access):
        vlob = await self._backend_conn.vlob_read(access["id"], access["rts"])

        author, raw = verify(sym_decrypt(access["key"], vlob))
        remote_manifest = pickle.loads(raw)
        local_manifest = remote_to_local_manifest(remote_manifest)
        self._local_db.set(access, local_manifest)
