from pyspark.sql.functions import col
from my_sbit_project.workflows.GenericUpserter import GenericUpserter


class UpserterGymLogins(GenericUpserter):
    def enrich_df(self, input_df):
        df_enriched = (
            input_df
            .withColumn("login", col("login").cast("timestamp"))
            .withColumn("logout", col("logout").cast("timestamp"))
            .select("mac_address", "gym", "login", "logout", "load_time")
        )
        return df_enriched
    
    def launch(self):
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)

        # read source
        df_source = self.read_deltatable_source()
        if self.watermark_eventtime:
            df_source.withWatermark(self.watermark_eventtime, self.watermark_delay)

        df = self.enrich_df(df_source)

        self.stream_upsert(df)