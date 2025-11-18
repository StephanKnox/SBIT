from pyspark.sql import DataFrame, functions as fn
from my_sbit_project.workflows.GenericUpserter import GenericUpserter


class UpserterUserBins(GenericUpserter):
    def enrich_df(self, input_df) -> DataFrame:
        df_user = self.spark.table(f"{self.env}.sbit_db.users").select("user_id")

        df_enriched = (
            input_df
            .join(df_user, ["user_id"], "left")
            .select("user_id", self.age_bins(fn.col("dob")),"gender", "city", "state")
        )
        return df_enriched
    
    def launch(self):
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)

        # read source
        df_source = self.read_deltatable_source(self.source_filter)

        df_enriched = self.enrich_df(df_source)

        if self.watermark_eventtime:
            df_enriched.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_enriched)


    def age_bins(self, dob_col):
        age_col = fn.floor(fn.months_between(fn.current_date(), dob_col)/12).alias("age")
        return (fn.when((age_col < 18), "under 18")
                .when((age_col >= 18) & (age_col < 25), "18-25")
                .when((age_col >= 25) & (age_col < 35), "25-35")
                .when((age_col >= 35) & (age_col < 45), "35-45")
                .when((age_col >= 45) & (age_col < 55), "45-55")
                .when((age_col >= 55) & (age_col < 65), "55-65")
                .when((age_col >= 65) & (age_col < 75), "65-75")
                .when((age_col >= 75) & (age_col < 85), "75-85")
                .when((age_col >= 85) & (age_col < 95), "85-95")
                .when((age_col >= 95), "95+")
                .otherwise("invalid age").alias("age"))