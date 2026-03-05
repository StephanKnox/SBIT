from datetime import datetime
from pyspark.sql import DataFrame, functions as fn
from delta.tables import DeltaTable
from my_sbit_project.utils.common import get_partition_values, remove_duplicates


class GenericBackfillWkf:
    def __init__(self, **kwargs):
        self.wkf_instance = kwargs.get("wkf_instance")
        self.dt_from = kwargs.get("dt_from")
        self.dt_to = kwargs.get("dt_to")
        self.dt_filter_col = kwargs.get("dt_filter_col")
        # TODO
        # Set in GenericUpseerter and dataclass schema
        self._upd_condition = None

        
    def launch(self):
        if not self.dt_to:
            self.dt_to = datetime.now().strftime("%Y-%m-%d")
        df = self.read_source(self.wkf_instance.source_filter)

        tgt_table = DeltaTable.forName(self.wkf_instance.spark, self.wkf_instance.sink_target)
        tgt_columns = tgt_table.toDF().columns

        upd_exprs = self.prep_upd_exprs(tgt_columns)

        partition_list = get_partition_values(df, self.dt_filter_col, self.dt_from, self.dt_to)
        print(partition_list)

        for i in range(0, len(partition_list), 1):
            dt_range = partition_list[i : i + 1]
            dt_1, dt_2 = dt_range[0], dt_range[-1]

            filter_expr = fn.expr(f"{self.dt_filter_col} BETWEEN DATE('{dt_1}') AND DATE('{dt_2}')")
            df_filtered = df.where(filter_expr)

            if df_filtered.isEmpty():
                print(f"DF row count is 0 after {filter_expr} and custom_backfill_filter()")
                continue

            if callable(getattr(self.wkf_instance, "parse_df", None)):
                df_filtered = self.parse_df(df_filtered)

            df_res = self.enrich_df(df_filtered)

            self.sink(df_res, tgt_table, upd_exprs)

            last_operationdf = tgt_table.history(1).select("version", "timestamp", "operation", "operationMetrics")
            print(last_operationdf)


    def read_source(self, filter_stmt=None) -> DataFrame:
        df_source = self.wkf_instance.spark.read.table(self.wkf_instance.source_path)
        if filter_stmt:
            df_source = df_source.filter(filter_stmt)
        return df_source
    
    def upd_column_expr(self, column):
        """
        Returns: column expression for condition update during merge"""
        if self._upd_condition:
            return fn.when(self._upd_condition, fn.col(f"source.{column}")).otherwise(fn.col(f"target.{column}"))
        # TODO
        # to test
        else:
            return fn.col(f"source.{column}")
    
    def prep_upd_exprs(self, tgt_columns):
        # E.g. tec_insert_date columns
        _cols_notto_upd = ["Col1", "Col2"]

        # For columns not matching by name between src and tgt
        _default_upd_exprs = {
            #"target.Col3": fn.col("source.ColA"),
            #"target.Col4": fn.col("source.ColB")
            }
        
        upd_exprs = {c: self.upd_column_expr(c) for c in tgt_columns if c not in _cols_notto_upd}
        upd_exprs_all = {**_default_upd_exprs, **upd_exprs}
        return upd_exprs_all


    def sink(self, df, tgt_table, upd_exprs):
        if self.wkf_instance.unique_cols:
            df = remove_duplicates(df, self.wkf_instance.unique_cols, self.wkf_instance.tiebreaker_cols)
        
        self.wkf_instance.merge(df, tgt_table, upd_exprs)


    def parse_df(self, df) -> DataFrame:
        return self.wkf_instance.parse_df(df)
    
    def enrich_df(self, df) -> DataFrame:
        return self.wkf_instance.enrich_df(df)

    def _custom_backfill_filter(self):
        pass
    
    def _optimize_src_table(self, table, predicate):
        pass