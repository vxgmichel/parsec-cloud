from abc import ABC


class Component(ABC):
    class Error(Exception):
        pass

    @abstractmethod
    def init(self):
        raise NotImplemented('Missing implementation of Component::init()')

    @abstractmethod
    def process(self):
        raise NotImplemented('Missing implementation of Component::process()')

    @abstractmethod
    def deinit(self):
        raise NotImplemented('Missing implementation of Component::init()')
