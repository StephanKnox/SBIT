from pyspark.sql import DataFrame, functions as fn
from pyspark.sql.types import StructField, StructType, LongType ,StringType, TimestampType, FloatType
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.utils.common import from_col_mapping_to_select
from my_sbit_project.utils.mapping import workouts_cols_mapping


json_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("workout_id", LongType(), True),
        StructField("timestamp", FloatType(), True), 
        StructField("action", StringType(), True),
        StructField("session_id", LongType(), True)
])


class UpserterWorkouts(GenericUpserter):
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("time", fn.col("timestamp").cast("timestamp"))
        )
        return df_enriched
    
    def launch(self):
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)

        # read source
        df_source = self.read_deltatable_source(self.source_filter)

        df_parsed = self.parse_df(df_source)

        df_parsed = self.enrich_df(df_parsed)

        if self.watermark_eventtime:
            df_parsed.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_parsed)

    def parse_df(self, df: DataFrame) -> DataFrame:
        select_cols = from_col_mapping_to_select(workouts_cols_mapping)
        df = df.withColumn("parsedJson", 
                           fn.from_json(fn.col("value").cast("string"), json_schema))
         
        df_parsed = df.select(*select_cols)
        return df_parsed