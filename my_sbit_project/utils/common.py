import re
from argparse import Namespace
import yaml
from dataclasses import is_dataclass, fields
from pyspark.sql import Window, SparkSession, functions as fn, Column
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
    return dict(vars(args))  # copy of a internal Namespace dict

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

def remove_duplicates(
    df, 
    unique_cols, 
    ordering=None, 
    default_order="desc"
):
    """
    Removes duplicate rows from a DataFrame based on columns that determine uniqueness
    and one or more ordering columns that define which record to keep.

    Params:
        df: input DataFrame
        unique_cols: list of columns to determine uniqueness
        ordering:
            - If a string → treated as single ordering column name
            - If a dict → treated as {column_name: "asc"|"desc"} for multi-column ordering
        default_order: ordering direction ("desc" or "asc") used when 'ordering' is a string
    Returns:
        DataFrame with duplicate rows removed
    """

    if isinstance(ordering, str):
        # Single column mode
        col_order = fn.col(ordering).desc() if default_order.lower() == "desc" else fn.col(ordering).asc()
        order_expr = [col_order]
    elif isinstance(ordering, dict):
        # Multi-column mode
        order_expr = [
            fn.col(col).desc() if direction.lower() == "desc" else fn.col(col).asc()
            for col, direction in ordering.items()
        ]
    else:
        ordering = None
        ##raise ValueError("Parameter 'ordering' must be either a column name (str) or a dict of {col: order}.")

    # Apply window spec and filter to retain the first row per group
    if ordering:
        win_spec = Window.partitionBy(*unique_cols).orderBy(*order_expr)
        df_with_rwn = df.withColumn("row_number", fn.row_number().over(win_spec))
        return df_with_rwn.filter("row_number = 1").drop("row_number")
    else:
        return df.dropDuplicates(subset=unique_cols)


def _add_missing_columns(df, cols):
        pass

def string_to_list(string, sep=","):
    """
    Splits a string into a list by the given separator, ignoring spaces around separators.
    
    Args:
        string (str): The input string to split.
        sep (str, optional): The separator to split by. Defaults to ','.
    
    Returns:
        list[str]: A list of trimmed substrings.
    """
    # Escape the separator for regex in case it's a special character (like '.')
    pattern = rf'\s*{re.escape(sep)}\s*'
    return re.split(pattern, string.strip())

def get_dates_from_df(df, partition_col, date_from, date_to) -> list:
    """
    Args:
    """
    filter_expr = (
        f"{partition_col} between DATE('{date_from}') and DATE('{date_to}')"
    )

    print(f"Date from: {date_from} and {date_to}")
    print(f"Filter expression: {filter_expr}")

    rows = (
        df.select(fn.col(partition_col))
          .where(filter_expr)
          .distinct()
          .orderBy(fn.col(partition_col))
          .collect()
    )
    
    return [row[partition_col] for row in rows]