from functools import wraps

def databricks_job_runner(func):
    """
    Generic decorator for Databricks Spark jobs.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        logger = self.logger

        logger.info("Job started")

        try:
            result = func(self, *args, **kwargs)
            logger.job_completed(success=True)
            return result

        except Exception as e:
            logger.error("Job execution failed", exception=e)
            logger.job_completed(success=False)
            raise

    return wrapper