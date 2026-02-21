from pyspark.sql import SparkSession, Row
from my_sbit_project.logging.LogHandler import LogHandler
from my_sbit_project.logging.LogRecord import LogRecord
from my_sbit_project.logging.LogLevel import LogLevel


class DeltaLogHandler(LogHandler):
    """
    Writes job-level summary records and error messages to a Delta table.
    Regular info/debug messages are skipped to keep the table lean.
    """

    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name

    def emit(self, record: LogRecord) -> None:
        # Only persist job-level summaries (have duration_ms) and errors
        is_job_summary = record.duration_ms is not None
        is_error = record.level == LogLevel.ERROR

        if not (is_job_summary or is_error):
            return

        row = Row(
            timestamp=record.timestamp,
            level=record.level.name,
            job_name=record.job_name,
            run_id=record.run_id,
            message=record.message,
            exception=record.exception,
            duration_ms=record.duration_ms,
        )

        df = self.spark.createDataFrame([row])
        df.write.format("delta").mode("append").saveAsTable(self.table_name)
