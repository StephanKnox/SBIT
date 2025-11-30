from abc import ABC, abstractmethod


class Backfill(ABC):
    @abstractmethod
    def launch(self):
        pass