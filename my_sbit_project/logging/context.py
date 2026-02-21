import os
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import uuid


class DatabricksContextResolver:
    """
    Resolves Azure Databricks job metadata from environment variables.
    """

    def resolve(self) -> Dict[str, Any]:
        return {
            "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
            "job_id": os.getenv("DATABRICKS_JOB_ID"),
            "job_run_id": os.getenv("DATABRICKS_RUN_ID"),
            "job_name": os.getenv("DATABRICKS_JOB_RUN_NAME"),
            "task_key": os.getenv("DATABRICKS_TASK_KEY"),
            "is_job_run": os.getenv("DATABRICKS_JOB_ID") is not None,
        }
    
class JobContext:
    """
    Job execution context enriched with Databricks metadata.
    """

    def __init__(self, runtime_context: Dict[str, Any]):
        self.runtime_context = runtime_context

        self.job_name = (
            runtime_context.get("job_name")
            or "interactive_run"
        )

        self.run_id = (
            runtime_context.get("job_run_id")
            or str(uuid.uuid4())
        )

        self.start_time = datetime.utcnow()
        self.end_time = None

    def finish(self):
        self.end_time = datetime.utcnow()

    @property
    def duration_sec(self) -> int:
        if not self.end_time:
            return 0
        return int((self.end_time - self.start_time).total_seconds())

    @property
    def duration_ms(self) -> int:
        return self.duration_sec * 1000