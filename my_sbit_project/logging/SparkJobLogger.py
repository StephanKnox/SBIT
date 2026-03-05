import datetime
from typing import List
from pyspark.sql import SparkSession, Row
from my_sbit_project.logging.context import JobContext
from my_sbit_project.logging.LogLevel import LogLevel
from my_sbit_project.logging.LogHandler import LogHandler
from my_sbit_project.logging.LogRecord import LogRecord
from my_sbit_project.logging.SparkMetricsCollector import SparkMetricsCollector


class SparkJobLogger:

    def __init__(
        self,
        context: JobContext,
        handlers: List[LogHandler],
        min_level: LogLevel = LogLevel.INFO,
        spark: SparkSession = None,
    ):
        self.context = context
        self.handlers = handlers
        self.min_level = min_level
        self.metrics_collector = (
            SparkMetricsCollector(spark) if spark else None
        )

    def _emit(self, record: LogRecord):
        for handler in self.handlers:
            handler.emit(record)

    def _log(self, level, message, exception=None, extra=None):
        if level.value < self.min_level.value:
            return

        record = LogRecord(
            timestamp=datetime.datetime.utcnow(),
            level=level,
            job_name=self.context.job_name,
            run_id=self.context.run_id,
            message=message,
            exception=exception,
            extra={
                **(extra or {}),
                **self.context.runtime_context,
            },
        )

        self._emit(record)

    def info(self, message, extra=None):
        self._log(LogLevel.INFO, message, extra=extra)

    def debug(self, message, extra=None):
        self._log(LogLevel.DEBUG, message, extra=extra)

    def error(self, message, exception=None, extra=None):
        self._log(
            LogLevel.ERROR,
            message,
            exception=str(exception) if exception else None,
            extra=extra,
        )

    def job_completed(self, success: bool):
        self.context.finish()

        metrics = (
            self.metrics_collector.collect()
            if self.metrics_collector
            else None
        )

        record = LogRecord(
            timestamp=datetime.datetime.utcnow(),
            level=LogLevel.INFO if success else LogLevel.ERROR,
            job_name=self.context.job_name,
            run_id=self.context.run_id,
            message="Job completed" if success else "Job failed",
            duration_ms=self.context.duration_ms,
            spark_metrics=metrics,
            extra=self.context.runtime_context,
        )

        self._emit(record)
