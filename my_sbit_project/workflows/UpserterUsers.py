from pyspark.sql.functions import col
from my_sbit_project.workflows.GenericUpserter import GenericUpserter
from my_sbit_project.logging.common import databricks_job_runner


class UpserterUsers(GenericUpserter):
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("registration_timestamp", col("registration_timestamp").cast("timestamp"))
            .select("user_id", "device_id", "mac_address", "registration_timestamp")
        )
        return df_enriched
    
    @databricks_job_runner
    def launch(self):
        self.logger.info(repr(self))

        # read source
        df_source = self.read_deltatable_source()
        if self.watermark_eventtime:
            df_source.withWatermark(self.watermark_eventtime, self.watermark_delay)

        df = self.enrich_df(df_source)

        self.stream_upsert(df)