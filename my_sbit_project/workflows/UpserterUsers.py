from pyspark.sql.functions import col
from my_sbit_project.workflows.GenericUpserter import GenericUpserter


class UpserterUsers(GenericUpserter):
    
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("registration_timestamp", col("registration_timestamp").cast("timestamp"))
            .select("user_id", "device_id", "mac_address", "registration_timestamp")
        )
        return df_enriched
    
    def launch(self):
        df_source = self.read_deltatable_source()
        df = self.enrich_df(df_source)
        
        if self.watermark_eventtime:
            df_source.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df)