from pyspark.sql import SparkSession
from my_sbit_project.utils.common import read_yaml, exec_sql

class SetupHelper():   
    def __init__(self, env, param):      
        self.catalog = env
        self.paramfile = param
        self.initialized = False
        self.app_name = 'SBIT'
        self.spark = self.get_spark_session()

    def get_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(self.app_name )
            .getOrCreate()
        )
        return spark

    def run(self):
        import time
        start = int(time.time())
        cfg = read_yaml(self.paramfile)
        tasks = cfg.get("job_config").get("tasks")
        print(f"\nStarting setup ...")
        
        for task_name, task_config in tasks.items():
            sql_stmt= task_config.get("sql_query")
            print(f"Executing {task_name}..", end='')
            exec_sql(self.spark, sql_stmt, {"catalog":self.catalog})
            print("Done")
        self.initialized = True
        print(f"Setup completed in {int(time.time()) - start} seconds")
