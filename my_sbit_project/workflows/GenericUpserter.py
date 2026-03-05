import time
from abc import ABC, abstractmethod
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as fn
from my_sbit_project.utils.workflow import DatabricksWorkflow
from my_sbit_project.utils.mapping import date_loader_cols_mapping
from my_sbit_project.utils.DatabricksStreamingMixin import DatabricksStreamingMixin
from my_sbit_project.utils.JobConfigStreaming import JobConfig
from my_sbit_project.utils.common import string_to_list, remove_duplicates


class GenericUpserter(DatabricksStreamingMixin, DatabricksWorkflow, ABC):
    job_config_class = JobConfig

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Source
        self.source_path = f"{self.env}.{self.job_cfg.source.path}"
        self.source_options = self.job_cfg.source.options
        self.source_filter = self.job_cfg.source.filter
        watermark = self.job_cfg.source.watermark
        if watermark:
            self.watermark_eventtime = watermark.get("event_time")
            self.watermark_delay = watermark.get("delay")
        else:
            self.watermark_eventtime = None
            self.watermark_delay = None
        # Sink
        self.sink_target = f"{self.env}.{self.job_cfg.sink.target}"
        self.sink_trigger = self.job_cfg.sink.trigger
        self.sink_options = self.job_cfg.sink.options
        # Streaming  
        streaming_options = self.job_cfg.streaming_options.options
        # Merging
        merge_details = self.job_cfg.params.merge
        self.merge_condition = merge_details.get("merge_condition")
        self.merge_upd_condition = merge_details.get("update_condition")
        self.delete_whenmatched = merge_details.get("delete_whenmatched", False)
        # Deduplication
        deduplication_details = self.job_cfg.params.deduplication
        if deduplication_details:
            self.unique_cols = string_to_list(deduplication_details.get("unique_cols"))
            self.tiebreaker_cols = deduplication_details.get("tiebreaker_cols")
        else:
            self.unique_cols  = None
            self.tiebreaker_cols = None

    @abstractmethod
    def launch(self):
        pass
    
    def upsert(self, df, epoch_id):
        self.logger.info(f"Starting to process epoch_id {epoch_id}")
        start_time = time.time()

        if self.unique_cols:
            df = remove_duplicates(df, self.unique_cols, self.tiebreaker_cols)

        target_table = DeltaTable.forName(self.spark, self.sink_target)
        target_columns = target_table.toDF().columns
        
        # TODO replace or remove
        default_update_exprs = {
            ##"target.CER_LAST_UPDATED_DATE": fn.col("source.CER_CREATION_DATE"),
            ##"target.CER_LAST_UPDATED_BY": fn.col("source.CER_CREATED_BY")
        }

        update_exprs = {}
        for col in target_columns:
            ##if col not in ['CER_CREATION_DATE', 'CER_CREATED_BY', 'CER_LAST_UPDATED_DATE', 'CER_LAST_UPDATED_BY']:
                if self.merge_upd_condition:
                    update_exprs[f"target.{col}"] = fn.when(fn.expr(self.merge_upd_condition),
                    fn.col(f"source.{col}")).otherwise(fn.col(f"target.{col}"))
                else:
                    update_exprs[f"target.{col}"] = fn.col(f"source.{col}")
        
        update_exprs = {**default_update_exprs, **update_exprs}

        self.merge(df, target_table, update_exprs)

        self.logger.info(f"foreachBatch execution finished. Execution time, seconds: {(time.time() - start_time)}")

        # TODO, retry functionality
        # if multiple jobs merge to the same table, merge with retries

        ##retry_count = 0
        ##while retry_count < self.max_retries:
        ##    try:
        ##        self.merge(df_parsed, update_exprs)
        ##        break
        ##    except ConcurrentAppendException as e:
        ##        retry_count += 1
        ##        self.retry_interval = self.retry_interval * 2 if retry_count == 5 else self.retry_interval
        ##        if retry_count < self.max_retries:
        ##            self.logger.warning(log_message=f"Error during upsert, attempt {retry_count}",
        ##                                status=STATUS_WARNING)
        ##            time.sleep(self.retry_interval)
        ##        else:
        ##            self.logger.error(log_message="Maximum retries reached. Exiting without successful upsert",
        ##                              status=STATUS_ERROR)
        ##           raise e
        ##self.logger.info(f"foreachBatch execution finished. Execution time, seconds: {(time.time() - start_time)}")

    def merge(self, df_source, target_table, update_exprs):

        if not self.delete_whenmatched:
            (target_table.alias("target").merge(df_source.alias("source"), self.merge_condition)
                .whenMatchedUpdate(set=update_exprs)
                .whenNotMatchedInsertAll().execute() )
        else:
            (target_table.alias("target").merge(df_source.alias("source"), self.merge_condition)
                .whenMatchedDelete().execute() )
            df_source.write.format("delta").mode("append").insertInto(self.sink_target)

    @abstractmethod
    def enrich_df(self, df) -> DataFrame:
        pass
