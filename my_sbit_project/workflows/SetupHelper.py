import time
from my_sbit_project.utils.workflow import DatabricksWorkflow


class SetupHelper(DatabricksWorkflow): 
      
    def __init__(self, **kwargs):      
        super().__init__(**kwargs)
        self.initialized = False

    def launch(self):
        start = int(time.time())

        cfg = self.app_cfg.get("job_config")
        tasks = cfg.get("tasks")
        print(f"\nStarting setup ...")
        
        for task_name, task_config in tasks.items():
            sql_stmt = task_config.get("sql_query")
            print(f"Executing {task_name}..", end='')
            self.executor.run(sql_stmt, in_args={"catalog":self.env})
            print("Done")
        self.initialized = True
        print(f"Setup completed in {int(time.time()) - start} seconds")
