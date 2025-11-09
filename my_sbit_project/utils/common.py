import yaml
from argparse import Namespace
from dataclasses import is_dataclass, fields
from pyspark.sql import SparkSession, functions as fn
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

def from_dict(data_class, data: dict):
    """
    Recursively instantiate a dataclass from a dict.
    Supports nested dataclasses.
    """
    if not is_dataclass(data_class):
        raise TypeError(f"{data_class} is not a dataclass type")

    fieldtypes = {f.name: f.type for f in fields(data_class)}
    init_kwargs = {}

    for key, field_type in fieldtypes.items():
        value = data.get(key)
        if is_dataclass(field_type):
            init_kwargs[key] = from_dict(field_type, value or {})
        else:
            init_kwargs[key] = value
    return data_class(**init_kwargs)

def from_col_mapping_to_select(col_mapping: dict) -> list:
    select_cols = [
            fn.col(col_info["json_path"]).cast(col_info["col_type"]).alias(col_name)
            for col_name, col_info in col_mapping.items()]
    return select_cols