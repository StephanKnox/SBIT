import GenericUpserter


class UpserterUsers(GenericUpserter):
    def enrich_df(input_df):
        df_enriched = (input_df
                       .withColumn("registration_timestamp", "cast(registration_timestamp as timestamp)")
                       .selec("user_id", "device_id", "mac_address", "registration_timestamp"))
        return df_enriched
    
    def launch(self):
        #print(f"Launching {self.__class__.__name__} with params: {self.__dict__}")
        print(self)

        # read source
        df_source = self.read_deltatable_source()
        if self.watermark_eventtime:
            df_source.withWatermark(self.watermark_eventtime, self.watermark_delay)

        df = self.enrich_df(df_source)

        (
        df.writeStream
        .trigger(**self.sink_trigger)
        .queryName(self.app_name)
        .options(**self.sink_options)
        .foreachBatch(lambda df, epoch_id: self.upsert(df, epoch_id))
        .start()
        .awaitTermination()
        )