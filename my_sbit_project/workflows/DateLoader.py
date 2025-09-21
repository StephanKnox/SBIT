from pyspark.sql import DataFrame, functions as fn
from pyspark.sql.types import StructType, StructField, DateType, StringType
from my_sbit_project.utils.workflow import DatabricksWorkflow
from my_sbit_project.utils.mapping import date_loader_cols_mapping
from my_sbit_project.utils.DatabricksStreamingMixin import DatabricksStreamingMixin
from my_sbit_project.utils.JobConfigStreaming import JobConfig


json_schema = StructType([
    StructField("date", DateType(), True),
    StructField("dayofmonth", StringType(), True),
    StructField("dayofweek", StringType(), True),
    StructField("dayofyear", StringType(), True),
    StructField("month", StringType(), True),
    StructField("week", StringType(), True),
    StructField("year", StringType(), True),
    StructField("week_part", StringType(), True),
])


class DateLoader(DatabricksStreamingMixin, DatabricksWorkflow):
    job_config_class = JobConfig

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Attributes from dataclass
        self.source_path = self.job_cfg.source.path
        self.source_options= self.job_cfg.source.options
        ##self.source_filter = self.job_cfg.source.filter
        self.sink_target= self.job_cfg.sink.target
        self.sink_trigger = self.job_cfg.sink.trigger
        self.sink_options = self.job_cfg.sink.options
        self.streaming_options = {}

    def __repr__(self):
        return "\n".join([f"{k}={v}" for k,v in self.__dict__.items()])

    def launch(self):
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)

        # read source
        df_source = self.read_files_source(json_schema)

        (
        df_source.writeStream
        .trigger(**self.sink_trigger)
        .queryName(self.app_name)
        .options(**self.sink_options)
        .foreachBatch(lambda df, epoch_id: self.append_batch_todelta(df, epoch_id, "overwrite"))
        .start()
        .awaitTermination()
        )
    
    def parse_df(self, df: DataFrame) -> DataFrame:
        select_cols = [
            fn.col(col_info["json_path"]).cast(col_info["col_type"]).alias(col_name)
            for col_name, col_info in date_loader_cols_mapping.items()]
         
        df_parsed = df.select(*select_cols)
        return df_parsed