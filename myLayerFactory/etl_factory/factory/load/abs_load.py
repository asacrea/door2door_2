import abc

class AbsLoad(abc.ABC):

    @property
    def process(self):
        return self._process

    @process.setter
    def load(self, process):
        self._process = process

    @abc.abstractmethod
    def load_s3(self):
        pass