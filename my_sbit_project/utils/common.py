import yaml
from pyspark.sql.utils import AnalysisException


def read_yaml(config_file):
    """Read configuration yaml"""
    with open(config_file, 'r') as cfg:
        config = yaml.safe_load(cfg)
    return config

def exec_sql(spark, sql_stmt, in_args=None):
    """Execute provides sql statement with optional args"""
    try:
        spark.sql(sql_stmt, args=in_args)
    except AnalysisException as e:
        print("Analysis error:", e)
    except Exception as e:
        print("Unexpected error:", e)