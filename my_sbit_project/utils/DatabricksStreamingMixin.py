import time
from pyspark.sql import DataFrame, functions as fn


class DatabricksStreamingMixin:
    
    def read_files_source(self, arg_schema) -> DataFrame:
        """Read file source via Autoloader and return a dataframe"""
        ## TO DO: where to put a filter statement???
        df_source = (self.spark.readStream
                     .format("cloudFiles")
                     .schema(arg_schema)
                     .options(**self.source_options)
                     .load(self.source_path))
        return df_source
    
    def read_deltatable_source(self, filter_stmt=None) -> DataFrame:
        """Read a delta table and return a dataframe"""
        df_source = (self.spark.readStream
                  .format("delta")
                  .options(**self.source_options)
                  .table(self.source_path)
        )
        if filter_stmt:
            df_source = df_source.filter(filter_stmt)
        return df_source


    def sink_batch_todelta(self, df, epoch_id, arg_mode= "append", to_parse=True):
        """Write micro batch function to be passed to .forEachBatch()"""
        print(f"Running micro-batch {epoch_id}")

        start_time = time.time()
        if to_parse:
            df = self.parse_df(df)

        df.write.format("delta").mode(arg_mode).saveAsTable(f"{self.env}.{self.sink_target}")
        print(f"foreachBatch execution finished. Execution time, seconds: {(time.time() - start_time)}")

    def add_file_meta_columns(self, df: DataFrame) -> DataFrame:   
        """Add metadata columns to the input dataframe"""
        df = df.withColumns({
                  "load_time": fn.current_timestamp(),
                  "source_file": fn.col("_metadata.file_name")})
        
        return df