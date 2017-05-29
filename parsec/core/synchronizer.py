from datetime import datetime

from parsec.tools import get_arg


# TODO merge block read et block stat

def synchronized_block_read(method):

    async def inner(*args, **kwargs):
        synchronizer = synchronizer_factory()
        block = None
        id = args[1]
        try:
            buffered_block = await synchronizer.get_buffered_block(id)
        except KeyError:
            block = await method(*args, **kwargs)
        else:
            block = await buffered_block.read()
        return block

    return inner


def synchronized_block_stat(method):

    async def inner(*args, **kwargs):
        synchronizer = synchronizer_factory()
        block = None
        id = args[1]
        try:
            buffered_block = await synchronizer.get_buffered_block(id)
        except KeyError:
            block = await method(*args, **kwargs)
        else:
            block = await buffered_block.stat()
        return block

    return inner


def synchronized_user_vlob(method):

    async def inner(*args, **kwargs):
        synchronizer = synchronizer_factory()
        user_vlob = None
        try:
            version = args[1]
        except IndexError:
            version = None
        try:
            buffered_user_vlob = await synchronizer.get_buffered_user_vlob(version)
        except KeyError:
            user_vlob = await method(*args, **kwargs)
        else:
            user_vlob = await buffered_user_vlob.read()
        return user_vlob

    return inner


def synchronized_vlob(method):

    async def inner(*args, **kwargs):
        synchronizer = synchronizer_factory()
        vlob = None
        id = args[1]
        try:
            version = args[3]
        except IndexError:
            version = None
        try:
            buffered_vlob = await synchronizer.get_buffered_vlob(id, version)
        except KeyError:
            vlob = await method(*args, **kwargs)
        else:
            vlob = await buffered_vlob.read()
        return vlob

    return inner


class synchronizer_factory():

    def __init__(self):
        self.buffered_blocks = {}
        self.buffered_user_vlobs = {}
        self.buffered_vlobs = {}
        self.synchronizables = []

    async def set_synchronizable_flag(self, buffered_object, boolean):
        if boolean and buffered_object not in self.synchronizables:
            self.synchronizables.append(buffered_object)
        elif not boolean and buffered_object in self.synchronizables:
            self.synchronizables.remove(buffered_object)

    async def get_buffered_block(self, id):
        return self.buffered_blocks[id]

    async def get_buffered_vlob(self, id, version=None):
        return self.buffered_vlobs[(id, version)]

    async def get_buffered_user_vlob(self, version=None):
        return self.buffered_user_vlobs[version]

    async def add_buffered_block(self, buffered_block):
        id = await buffered_block.get_id()
        self.buffered_blocks[id] = buffered_block

    async def add_buffered_vlob(self, buffered_vlob):
        id = await buffered_vlob.get_id()
        version = await buffered_vlob.get_version()
        self.buffered_vlobs[(id, version)] = buffered_vlob
        self.buffered_vlobs[(id, None)] = buffered_vlob

    async def add_buffered_user_vlob(self, buffered_user_vlob):
        version = await buffered_user_vlob.get_version()
        self.buffered_user_vlobs[version] = buffered_user_vlob
        self.buffered_user_vlobs[None] = buffered_user_vlob

    async def remove_buffered_block(self, buffered_block):
        id = await buffered_block.get_id()
        del self.buffered_blocks[id]

    async def remove_buffered_vlob(self, buffered_vlob):
        id = await buffered_vlob.get_id()
        version = await buffered_vlob.get_version()
        del self.buffered_vlobs[(id, version)]
        del self.buffered_vlobs[(id, None)]

    async def remove_buffered_user_vlob(self, buffered_user_vlob):
        version = await buffered_user_vlob.get_version()
        del self.buffered_user_vlobs[version]
        del self.buffered_user_vlobs[None]

    async def commit(self):
        for buffered_block in self.buffered_blocks.values():
            if buffered_block in self.synchronizables:
                await buffered_block.commit()
                await self.set_synchronizable_flag(buffered_block, False)
        for buffered_user_vlob in self.buffered_user_vlobs.values():
            if buffered_user_vlob in self.synchronizables:
                await buffered_user_vlob.commit()
                await self.set_synchronizable_flag(buffered_user_vlob, False)
        for buffered_vlob in self.buffered_vlobs.values():
            if buffered_vlob in self.synchronizables:
                await buffered_vlob.commit()
                await self.set_synchronizable_flag(buffered_vlob, False)

    async def periodic_commit(self):
        # TODO
        pass

    async def discard(self):
        for key, buffered_block in self.buffered_blocks.items():
            await buffered_block.discard()
            await self.remove_block(*key)
        for key, buffered_user_vlob in self.buffered_user_vlobs.items():
            await buffered_user_vlob.discard()
            await self.remove_user_vlob(key)
        for key, buffered_vlob in self.buffered_vlobs.items():
            await buffered_vlob.discard()
            await self.remove_vlob(key)


def synchronizer_factory(singleton=synchronizer_factory()):
    return singleton
