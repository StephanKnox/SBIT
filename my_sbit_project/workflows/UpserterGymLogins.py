import GenericUpserter

class UpserterGymLogins(GenericUpserter):
    def enrich_df(input_df):
        df_enriched = (input_df
                       .withColumn("registration_timestamp", "cast(registration_timestamp as timestamp)")
                       .selec("user_id", "device_id", "mac_address", "registration_timestamp"))
        return df_enriched