from abc import ABC, abstractmethod
from dataclasses import is_dataclass
from my_sbit_project.utils.common import ConfigLoader, SparkSessionFactory, SqlExecutor, from_dict

class DatabricksWorkflow(ABC):
    def __init__(self, **kwargs):
        self.env = kwargs.get("env")
        self.app_name = kwargs.get("app")
        self.app_cfg = ConfigLoader.read_config(kwargs.get("app_cfg"))
        self.spark = SparkSessionFactory.create(self.app_name, self.env)
        #self.spark = None
        self.executor = SqlExecutor(self.spark)

        # Parse job_config if subclass declares a dataclass
        job_cfg_dict = self.app_cfg.get("job_config", {})
        if self.job_config_class is not None:
            if not is_dataclass(self.job_config_class):
                raise TypeError("job_config_class must be a dataclass")
            self.job_cfg = from_dict(self.job_config_class, job_cfg_dict)
        else:
            self.job_cfg = job_cfg_dict  # raw dict fallback

    def __repr__(self):
        parts = [ f"{k}={v!r}" for k,v in vars(self).items() if not (k.startswith("_") or callable(v))]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    ##@abstractmethod
    ##def launch(self):
    ##    pass