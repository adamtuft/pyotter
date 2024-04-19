from abc import ABC, abstractmethod

from otter.log import Loggable


class WriterBase(ABC, Loggable):

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex, tb):
        self.close()
        return ex_type is not None

    @abstractmethod
    def close(self): ...
