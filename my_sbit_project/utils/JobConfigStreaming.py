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

@dataclass
class JobConfig:
    sink: SinkConfig
    source: SourceConfig
