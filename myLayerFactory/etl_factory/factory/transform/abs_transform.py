import abc

class AbsTransform(abc.ABC):

    @abc.abstractmethod
    def execute(self):
        pass
