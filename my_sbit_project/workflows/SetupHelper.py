import time
from my_sbit_project.utils.workflow import DatabricksWorkflow
from my_sbit_project.logging.common import databricks_job_runner


class SetupHelper(DatabricksWorkflow):
    job_config_class = None

    def __init__(self, **kwargs):      
        super().__init__(**kwargs)
        self.initialized = False

    @databricks_job_runner
    def launch(self):
        start = int(time.time())

        cfg = self.app_cfg.get("job_config")
        tasks = cfg.get("tasks")
        self.logger.info("Starting setup ...")
        
        for task_name, task_config in tasks.items():
            sql_stmt = task_config.get("sql_query")
            self.logger.info(f"Executing {task_name}..")
            self.executor.run(sql_stmt, in_args={"catalog":self.env})
            self.logger.info(f"{task_name} done")
        self.initialized = True
        self.logger.info(f"Setup completed in {int(time.time()) - start} seconds")
