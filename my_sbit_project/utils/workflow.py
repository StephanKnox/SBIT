from abc import ABC
from dataclasses import is_dataclass
from my_sbit_project.utils.common import ConfigLoader, SparkSessionFactory, SqlExecutor, from_dict
from my_sbit_project.logging import (
    LogLevel, 
    SparkJobLogger,
    ConsoleLogHandler,
    DeltaLogHandler,
    DatabricksContextResolver,
    JobContext)


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

        # Initialize logger
        self.logger = self._build_logger()

    def __repr__(self):
        parts = [ f"{k}={v!r}" for k,v in vars(self).items() if not (k.startswith("_") or callable(v))]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def _build_logger(self) -> SparkJobLogger:
        """Build a SparkJobLogger from the logging section in YAML config.

        If the YAML omits the `logging:` section entirely, falls back to
        console-only logging at INFO level.
        """
        # Resolve logging config — may be None or have all-None fields
        log_cfg = getattr(self.job_cfg, "logging", None) if self.job_cfg else None

        handler_names = None
        min_level_str = None
        options = {}

        if log_cfg is not None:
            handler_names = getattr(log_cfg, "handlers", None)
            min_level_str = getattr(log_cfg, "min_level", None)
            options = getattr(log_cfg, "options", None) or {}

        # Determine log level (default: INFO)
        try:
            min_level = LogLevel[min_level_str] if min_level_str else LogLevel.INFO
        except KeyError:
            min_level = LogLevel.INFO

        # Build handler list based on YAML (default: console only)
        handlers = []
        if handler_names:
            for name in handler_names:
                if name == "console":
                    handlers.append(ConsoleLogHandler())
                elif name == "delta_table":
                    delta_table = options.get("delta_table")
                    if delta_table:
                        full_table = f"{self.env}.{delta_table}"
                        handlers.append(DeltaLogHandler(self.spark, full_table))
        if not handlers:
            handlers.append(ConsoleLogHandler())

        # Create context and logger
        runtime_ctx = DatabricksContextResolver().resolve()
        context = JobContext(runtime_ctx)
        return SparkJobLogger(
            context=context,
            handlers=handlers,
            min_level=min_level,
            spark=self.spark,
        )
