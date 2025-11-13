from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, DoubleType
from my_sbit_project.utils.workflow import DatabricksWorkflow
from my_sbit_project.utils.DatabricksStreamingMixin import DatabricksStreamingMixin
from my_sbit_project.utils.JobConfigStreaming import JobConfig


json_schema = StructType([
    StructField("mac_address", StringType(), True),
    StructField("gym", LongType(), True),
    StructField("login", DoubleType(), True),
    StructField("logout", DoubleType(), True),
])


class ConsumerGymLogins(DatabricksStreamingMixin, DatabricksWorkflow):
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

    def launch(self):
        print(self)

        # read source
        df_source = self.read_files_source(json_schema)
        # add metadata columns
        df_source = self.add_file_meta_columns(df_source)

        # TODO parsing is not needed?
        (
        df_source.writeStream
        .trigger(**self.sink_trigger)
        .queryName(self.app_name)
        .options(**self.sink_options)
        .foreachBatch(lambda df, epoch_id: self.sink_batch_todelta(df, epoch_id, to_parse=False))
        .start()
        .awaitTermination()
        )
    
    #def parse_df(self, df: DataFrame) -> DataFrame:
    #    select_cols = from_col_mapping_to_select(date_loader_cols_mapping)
    #    df_parsed = df.select(*select_cols)
        
    #    return df_parsed
    