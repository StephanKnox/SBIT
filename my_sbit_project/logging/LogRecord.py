from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from my_sbit_project.logging.LogLevel import LogLevel


@dataclass(frozen=True)
class LogRecord:
    timestamp: datetime
    level: LogLevel
    job_name: str
    run_id: str
    message: str
    exception: Optional[str] = None
    duration_ms: Optional[int] = None
    spark_metrics: Optional[Dict[str, Any]] = None
    extra: Optional[Dict[str, Any]] = None