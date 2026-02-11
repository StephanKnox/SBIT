from pyspark.sql import DataFrame, functions as fn
from my_sbit_project.workflows.GenericUpserter import GenericUpserter


class UpserterCompletedWorkouts(GenericUpserter):
    
    def enrich_df(self, df_start) -> DataFrame:
        df_start = df_start.selectExpr("user_id", "workout_id", "session_id", "time as start_time")

        df_stop = self.read_deltatable_source(filter_stmt="action = 'stop'")
        df_stop = df_stop.selectExpr("user_id", "workout_id", "session_id", "time as end_time")

        join_condition = [df_start.user_id == df_stop.user_id, df_start.workout_id==df_stop.workout_id,
                           df_start.session_id==df_stop.session_id, 
                          df_stop.end_time < df_start.start_time + fn.expr('interval 3 hour')]         
        
        df_enriched = (df_start.join(df_stop, join_condition)
                            .select(df_start.user_id, df_start.workout_id, df_start.session_id, df_start.start_time, df_stop.end_time)
                   )
        
        return df_enriched
    
    def launch(self):
        df_source = self.read_deltatable_source(self.source_filter)
        df_enriched = self.enrich_df(df_source)

        if self.watermark_eventtime:
            df_enriched.withWatermark(self.watermark_eventtime, self.watermark_delay)

        self.stream_upsert(df_enriched)
