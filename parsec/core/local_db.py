from pathlib import Path


class LocalDBError(Exception):
    pass


class LocalDBMissingEntry(LocalDBError):
    def __init__(self, access):
        self.access = access


class LocalDB:
    def __init__(self, path):
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)

        # TODO: fix recursive import
        from parsec.core.encryption_manager import encrypt_for_local, decrypt_for_local

        self._encrypt_for_local = encrypt_for_local
        self._decrypt_for_local = decrypt_for_local

    @property
    def path(self):
        return str(self._path)

    def get(self, access):
        file = self._path / access["id"]
        try:
            raw = file.read_bytes()
        except FileNotFoundError:
            raise LocalDBMissingEntry(access)
        return self._decrypt_for_local(access['key'], raw)

    def set(self, access, manifest):
        ciphered = self._encrypt_for_local(access['key'], manifest)
        file = self._path / access["id"]
        file.write_bytes(ciphered)

    def clear(self, access):
        file = self._path / access["id"]
        try:
            file.unlink()
        except FileNotFoundError:
            pass
