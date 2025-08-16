import yaml
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


class DatabricksWorkflow(ABC):
    def __init__(self, env, app_cgf):
        self.env = env
        self.app_cfg = self.read_yaml(app_cgf)
        self.spark = self.get_spark_session()
        #self.glb_cfg = glb_cfg
        
    def get_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(self.__class__.__name__)
        )
        if self.env == "LOCAL":
        # For testing purposes only on local env
            spark.master("local[*]")
        
        # Reading non-default spark session config options
        ##for k, v in self.global_config.get("sparksession").get("config", {}).items():
        ##    spark.config(k, v)

        return spark.getOrCreate()

    def exec_sql(self, sql_stmt, in_args=None):
        """Execute provided sql statement with optional args"""
        try:
            self.spark.sql(sql_stmt, args=in_args)
        except AnalysisException as e:
            # TO DO: replace prints with logging
            print(f"Execution error when running SQL: {sql_stmt}")
            raise

    @staticmethod
    def read_yaml(cfg_file):
        """Read configuration yaml"""
        try:
            with open(cfg_file) as cfg:
                config = yaml.safe_load(cfg)    
            return config
        except FileNotFoundError as e:
            # TO DO: replace prints with logging
            print(f"""YAML file was not found at {cfg_file} or you dont have permissions
                  to access it""")
            raise
        
    @abstractmethod
    def launch():
        pass