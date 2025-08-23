import yaml
from argparse import Namespace
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


class ConfigLoader:
    @staticmethod
    def _read_yaml(cfg_file):
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

    @staticmethod
    def read_config(cfg_file) -> dict:
        # TODO: handle exception here or in _read_yaml ?
        return ConfigLoader._read_yaml(cfg_file)
    

class SparkSessionFactory:
    @staticmethod
    def create(app_name, env) -> SparkSession:
        builder = SparkSession.builder.appName(app_name)
        if env == "LOCAL":
            builder = builder.master("local[*]")
        return builder.getOrCreate()


class SqlExecutor:
    def __init__(self, spark):
        self.spark = spark

    def run(self, sql_stmt, in_args=None):
        try:
            return self.spark.sql(sql_stmt, args=in_args)
        except AnalysisException as e:
            # TODO: replace print with logging
            print(f"SQL execution failed: {sql_stmt}")
            raise

def parse_wkf_args(args: Namespace) -> dict:
    return vars(args)