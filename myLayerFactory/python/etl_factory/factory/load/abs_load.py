import abc

class AbsLoad(abc.ABC):

    @abc.abstractmethod
    def execute(self):
        pass