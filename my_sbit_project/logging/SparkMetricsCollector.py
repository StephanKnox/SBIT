from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, Row

class SparkMetricsCollector:
    """
    Collects safe, driver-level Spark metrics.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def collect(self) -> Dict[str, Any]:
        sc = self.spark.sparkContext
        executor_infos = sc._jsc.sc().statusTracker().getExecutorInfos()

        executors = [
            {
                "executor_id": e.executorId(),
                "host": e.host(),
                "total_cores": e.totalCores(),
                "max_memory": e.maxMemory(),
            }
            for e in executor_infos
            if e.executorId() != "driver"
        ]

        return {
            "application_id": sc.applicationId,
            "application_name": sc.appName,
            "executor_count": len(executors),
            "executors": executors,
        }
