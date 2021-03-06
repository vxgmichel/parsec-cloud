from hashlib import sha256

from parsec.crypto import decrypt_raw_with_secret_key
from parsec.core.schemas import loads_manifest, dumps_manifest
from parsec.core.fs.utils import remote_to_local_manifest
from parsec.core.fs.types import BlockAccess, Access


class RemoteLoader:
    def __init__(self, backend_cmds, encryption_manager, local_db):
        self.backend_cmds = backend_cmds
        self.encryption_manager = encryption_manager
        self.local_db = local_db

    async def load_block(self, access: BlockAccess) -> None:
        """
        Raises:
            BackendConnectionError
            CryptoError
        """
        ciphered_block = await self.backend_cmds.blockstore_read(access["id"])
        # TODO: let encryption manager do the digest check ?
        # TODO: is digest even useful ? Given nacl.secret.Box does digest check
        # on the ciphered data they cannot be tempered. And given each block
        # has an unique key, valid blocks cannot be switched together.
        # TODO: better exceptions
        block = decrypt_raw_with_secret_key(access["key"], ciphered_block)
        assert sha256(block).hexdigest() == access["digest"], access

        self.local_db.set(access, block)

    async def load_manifest(self, access: Access) -> None:
        _, blob = await self.backend_cmds.vlob_read(access["id"], access["rts"])
        raw_remote_manifest = await self.encryption_manager.decrypt_with_secret_key(
            access["key"], blob
        )
        # TODO: handle and/or document exceptions
        remote_manifest = loads_manifest(raw_remote_manifest)
        local_manifest = remote_to_local_manifest(remote_manifest)
        raw_local_manifest = dumps_manifest(local_manifest)

        self.local_db.set(access, raw_local_manifest)
