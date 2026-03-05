from pyspark.sql import DataFrame, functions as fn
from pyspark.sql.types import StructField, StructType, LongType ,StringType, TimestampType, DateType
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.utils.constants import address_struct
from my_sbit_project.utils.common import from_col_mapping_to_select
from my_sbit_project.utils.mapping import user_profiles_cols_mapping
from my_sbit_project.logging.common import databricks_job_runner


json_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("update_type", StringType(), True),
        StructField("timestamp", TimestampType(), True), 
        StructField("dob", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("address", address_struct, True),
])

class UpserterUserProfiles(GenericUpserter):
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("dob", fn.to_date('dob','MM/dd/yyyy'))
            .withColumn("updated", fn.col("timestamp").cast("timestamp"))
            #.select("user_id", "dob", "sex", "gender", "first_name", "last_name", )
        )
        return df_enriched
    
    
    @databricks_job_runner
    def launch(self):
        self.logger.info(repr(self))

        # read source
        df_source = self.read_deltatable_source(self.source_filter)

        df_parsed = self.parse_df(df_source)

        df_parsed = self.enrich_df(df_parsed)

        if self.watermark_eventtime:
            df_parsed.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_parsed)

    def parse_df(self, df: DataFrame) -> DataFrame:
        select_cols = from_col_mapping_to_select(user_profiles_cols_mapping)
        df = df.withColumn("parsedJson", 
                           fn.from_json(fn.col("value").cast("string"), json_schema))
         
        df_parsed = df.select(*select_cols)
        return df_parsed