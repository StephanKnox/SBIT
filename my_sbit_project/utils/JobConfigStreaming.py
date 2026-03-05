from dataclasses import dataclass


@dataclass
class SinkConfig:
    target: str
    trigger: dict
    options: dict

@dataclass
class SourceConfig:
    path: str
    filter: str
    options: dict
    watermark: dict

@dataclass
class Params:
    deduplication: dict
    merge: dict

@dataclass
class StreamingOptions:
    options: dict

@dataclass
class BackfillConfig:
    options: dict

@dataclass
class LoggingConfig:
    min_level: str
    handlers: list
    options: dict

@dataclass
class JobConfig:
    sink: SinkConfig
    source: SourceConfig
    params: Params
    streaming_options: StreamingOptions
    backfill: BackfillConfig
    logging: LoggingConfig
