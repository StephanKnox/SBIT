import time
from pyspark.sql import DataFrame


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

    def append_batch_todelta(self, df, epoch_id, arg_mode= "append", to_parse=True):
        """Write micro batch function to be passed to .forEachBatch()"""
        print(f"Running micro-batch {epoch_id}")

        start_time = time.time()
        if to_parse:
            df = self.parse_df(df)

        df.write.format("delta").mode(arg_mode).saveAsTable(f"{self.env}.{self.sink_target}")
        print(f"foreachBatch execution finished. Execution time, seconds: {(time.time() - start_time)}")
