from abc import ABC, abstractmethod
from my_sbit_project.utils.common import ConfigLoader, SparkSessionFactory, SqlExecutor

class DatabricksWorkflow(ABC):
    def __init__(self, **kwargs):
        self.env = kwargs.get("env")
        self.app_name = kwargs.get("app")
        self.app_cfg = ConfigLoader.read_config(kwargs.get("app_cfg"))
        self.spark = SparkSessionFactory.create(self.app_name, self.env)
        self.executor = SqlExecutor(self.spark)

    @abstractmethod
    def launch(self):
        pass