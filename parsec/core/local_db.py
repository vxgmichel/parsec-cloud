class LocalDBError(Exception):
    pass


class LocalDBMissingEntry(LocalDBError):
    def __init__(self, access):
        super().__init__(access)
        self.access = access


class LocalDB:
    def __init__(self, path):
        raise NotImplementedError()
