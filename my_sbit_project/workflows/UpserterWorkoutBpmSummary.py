from pyspark.sql import DataFrame, functions as fn
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.logging.common import databricks_job_runner


class UpserterWorkoutBpmSummary(GenericUpserter):

    def enrich_df(self, df_input) -> DataFrame:
        df_users = self.spark.read.table(f"{self.env}.sbit_db.user_bins")

        if self.watermark_eventtime:
            df_enriched = df_input.withWatermark(self.watermark_eventtime, self.watermark_delay)

        df_res= (
            df_enriched.groupBy("user_id", "workout_id", "session_id", "end_time")
                    .agg(fn.min("heartrate").alias("min_bpm"), fn.mean("heartrate").alias("avg_bpm"), 
                         fn.max("heartrate").alias("max_bpm"), fn.count("heartrate").alias("num_recordings"))                         
                    .join(df_users, ["user_id"])
                    .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings", "end_time")
        )
        return df_res
    
    @databricks_job_runner
    def launch(self):
        self.logger.info(repr(self))

        # read source
        df_source = self.read_deltatable_source()
        df_enriched = self.enrich_df(df_source)
 
        if self.watermark_eventtime:
            df_enriched = df_enriched.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_enriched)