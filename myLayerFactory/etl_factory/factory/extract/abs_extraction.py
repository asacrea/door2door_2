import abc

class AbsExtraction(abc.ABC):

    @abc.abstractmethod
    def extract(self):
        pass