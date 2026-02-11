from pyspark.sql import DataFrame, functions as fn
from pyspark.sql.types import StructField, StructType, LongType, TimestampType, DoubleType
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.utils.common import from_col_mapping_to_select
from my_sbit_project.utils.mapping import bpm_cols_mapping


json_schema = StructType([
        StructField("device_id", LongType(), True),
        StructField("time", TimestampType(), True), 
        StructField("heartrate", DoubleType(), True)
])


class UpserterHeartRate(GenericUpserter):

    def enrich_df(self, input_df)-> DataFrame:
        df_enriched = (
            input_df
            .withColumn("valid", fn.when(fn.col("heartrate") <= 0, False).otherwise(True))
        )
        return df_enriched
    
    def launch(self):
        df_source = self.read_deltatable_source(self.source_filter)
        df_parsed = self.parse_df(df_source)

        df_parsed = self.enrich_df(df_parsed)

        if self.watermark_eventtime:
            df_parsed.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_parsed)

    def parse_df(self, df) -> DataFrame:
        select_cols = from_col_mapping_to_select(bpm_cols_mapping)
        df = df.withColumn("parsedJson", 
                           fn.from_json(fn.col("value").cast("string"), json_schema))
         
        df_parsed = df.select(*select_cols)
        return df_parsed
    