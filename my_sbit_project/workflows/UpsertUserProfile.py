# TODO, needed or not?

import time
from pyspark.sql import DataFrame, functions as fn
from pyspark.sql.types import StructField, StructType, LongType ,StringType, TimestampType, DateType
from my_sbit_project.utils.workflow import DatabricksWorkflow
from my_sbit_project.utils.constants import address_struct
from my_sbit_project.utils.DatabricksStreamingMixin import DatabricksStreamingMixin
from my_sbit_project.utils.JobConfigStreaming import JobConfig


json_schema = StructType([
    StructField("key", LongType(), True),
    StructField("value", StructType([
        StructField("user_id", LongType(), True),
        StructField("update_type", StringType(), True),
        StructField("timestamp", TimestampType(), True), 
        StructField("dob", DateType(), True),
        StructField("sex", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("address", address_struct, True),
    ]), True),
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True),  
])


class UserInfoLoader(DatabricksStreamingMixin, DatabricksWorkflow):
    job_config_class = JobConfig()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Attributes from dataclass
        self.source_path = self.job_cfg.source.path
        self.source_filter = self.job_cfg.source.filter
        self.sink_target= self.job_cfg.sink.target
        self.sink_trigger = self.job_cfg.sink.trigger
        self.sink_options = self.job_cfg.sink.options
        self.streaming_options = {}

    def __repr__(self):
        return "\n".join([f"{k}={v}" for k,v in self.__dict__.items()])

    def launch(self):
        """TO DO"""
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)
        print(f"Date Loader mapping {date_loader_cols_mapping}")

        ## TO DO: Where to put streaming options? and how?
        # read source
        ##df_source = DatabricksStreamingMixin.read_files_source(json_schema)
        ##print(f"Trigger: {self.sink_trigger}")
        ##print(f"Sink options: {self.sink_options}")

        ##(
        ##df_source.writeStream.format("delta")
        ##.trigger(**self.trigger)
        ##.queryName(self.app_name)
        ##.option(**self.sink_options)
        ##.foreachBatch(
        ##    lambda df, epoch_id: DatabricksStreamingMixin.append_batch_todelta(df, epoch_id, "overwrite"))
        ##.start().awaitTermination()
        ##)

    def parse_df(self, df: DataFrame) -> DataFrame:
        select_cols = from_col_mapping_to_select(date_loader_cols_mapping)
         
        df_parsed = df.select(*select_cols)
        return df_parsed


    def transform_df(self, df: DataFrame) -> DataFrame:
        df = df.withColumns(
            {"col1": fn.to_date(fn.col("date_col"), "SHORT_DATE_FORMAT"),
             "col2": fn.upper(fn.col("col2"))
             }
        )
        return df