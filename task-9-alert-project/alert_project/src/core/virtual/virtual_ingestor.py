from abc import ABC, abstractmethod


class BaseIngestor(ABC):
    def __init__(self, endpoint, chunk_size=100000):
        self.endpoint = endpoint
        self.chunk_size = chunk_size

    @abstractmethod
    def load_chunks(self, **kwargs: any):
        pass
