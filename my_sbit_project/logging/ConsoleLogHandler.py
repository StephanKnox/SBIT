import json
from my_sbit_project.logging.LogHandler import LogHandler
from my_sbit_project.logging.LogRecord import LogRecord


class ConsoleLogHandler(LogHandler):
    
    def emit(self, record: LogRecord) -> None:
        print(json.dumps({
            "timestamp": record.timestamp.isoformat(),
            "level": record.level.name,
            "job_name": record.job_name,
            "run_id": record.run_id,
            "message": record.message,
            "exception": record.exception,
            "duration_ms": record.duration_ms,
            "spark_metrics": record.spark_metrics,
            "extra": record.extra,
        }))