from abc import ABC, abstractmethod
from my_sbit_project.logging.LogRecord import LogRecord

class LogHandler(ABC):

    @abstractmethod
    def emit(self, record: LogRecord) -> None:
        pass