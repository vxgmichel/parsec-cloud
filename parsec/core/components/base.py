from abc import ABC, abstractmethod


class Component(ABC):
    class Error(Exception):
        pass

    @abstractmethod
    def init(self):
        raise NotImplementedError(
            'Missing implementation of Component::init()'
        )

    @abstractmethod
    def process(self):
        raise NotImplementedError(
            'Missing implementation of Component::process()'
        )

    @abstractmethod
    def deinit(self):
        raise NotImplementedError(
            'Missing implementation of Component::init()'
        )


class ComponentNames:
    ENTRYPOINT = 'entry'
    USER_MANIFEST_SERVICE = 'umsvc'
    FILE_BLOCK_SERVICE = 'fbsvc'
    SYNCHRONIZER = 'sync'
    ENDPOINT = 'end'
    REPLY = 'reply'
