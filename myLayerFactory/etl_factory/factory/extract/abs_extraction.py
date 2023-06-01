import abc

class AbsExtraction(abc.ABC):

    @property
    def process(self):
        return self._process
    
    @process.setter
    def extraction(self, process):
        self._process = process

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def extract(self):
        pass