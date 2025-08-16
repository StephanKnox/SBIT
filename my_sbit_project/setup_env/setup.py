from my_sbit_project.utils.common import DatabricksWorkflow, exec_sql


class SetupHelper(DatabricksWorkflow):   
    def __init__(self, env, app_cfg):      
        super.__init__(env, app_cfg)
        #self.catalog = env
        #self.paramfile = param
        self.initialized = False
        #self.app_name = 'SBIT'
        #self.spark = self.get_spark_session()

    def launch(self):
        import time
        start = int(time.time())

        cfg = self.app_cfg.get("job_config")
        tasks = cfg.get("tasks")
        print(f"\nStarting setup ...")
        
        for task_name, task_config in tasks.items():
            sql_stmt= task_config.get("sql_query")
            print(f"Executing {task_name}..", end='')
            exec_sql(self.spark, sql_stmt, {"catalog":self.catalog})
            print("Done")
        self.initialized = True
        print(f"Setup completed in {int(time.time()) - start} seconds")
