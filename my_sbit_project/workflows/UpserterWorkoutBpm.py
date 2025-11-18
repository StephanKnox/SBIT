from pyspark.sql import DataFrame, functions as fn
from my_sbit_project.workflows.GenericUpserter import GenericUpserter


class UpserterWorkoutBpm(GenericUpserter):
    def enrich_df(self, df_input ) -> DataFrame:
        df_users = self.spark.read.table(f"{self.env}.sbit_db.users")
        df_completed_workouts = (df_input
                       .join(df_users, "user_id")
                       .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
        )

        df_bpm = (self.spark.readStream
                       .option("skipChangedCommits", True)
                       .table(f"{self.env}.sbit_db.heart_rate")
                       .filter("valid = True")                         
                       .selectExpr("device_id", "time", "heartrate")
                       .withWatermark("time", "30 seconds")
                   )

        join_condition =  [df_completed_workouts.device_id == df_bpm.device_id, 
                          df_bpm.time > df_completed_workouts.start_time, df_bpm.time <= df_completed_workouts.end_time,
                          df_completed_workouts.end_time < df_bpm.time + fn.expr('interval 3 hour')]       
        
        df_enriched = (df_bpm.join(df_completed_workouts, join_condition)
                          .select("user_id", "workout_id","session_id", "start_time", "end_time", "time", "heartrate")
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
