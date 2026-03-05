from pyspark.sql.functions import col
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.logging.common import databricks_job_runner


class UpserterGymLogins(GenericUpserter):
    
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("login", col("login").cast("timestamp"))
            .withColumn("logout", col("logout").cast("timestamp"))
            .select("mac_address", "gym", "login", "logout", "load_time")
        )
        return df_enriched
    
    @databricks_job_runner
    def launch(self):
        self.logger.info(repr(self))

        # read source
        df_source = self.read_deltatable_source()
        df = self.enrich_df(df_source)

        if self.watermark_eventtime:
            df_source.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df)