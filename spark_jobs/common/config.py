"""
Centralized configuration for Spark jobs
"""

from dataclasses import dataclass
from typing import Optional
import os
@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092')
    schema_registry_url: str = os.environ.get('KAFKA_SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
    topic_prefix: str = os.environ.get('KAFKA_TOPIC_PREFIX', 'dbserver1')

@dataclass
class IcebergConfig:
    catalog_name:str=os.environ.get('ICEBERG_CATALOG_NAME', 'docstream_catalog')
    namespace_events: str = os.environ.get('ICEBERG_NAMESPACE_EVENTS', 'events')
    namespace_metrics: str = os.environ.get('ICEBERG_NAMESPACE_METRICS', 'metrics')
    table_documents: str = "documents"
    table_metrics: str = "hourly"

@dataclass
class ElasticsearchConfig:
    host: str = os.environ.get('ELASTICSEARCH_HOST', 'elasticsearch')
    port: int = os.environ.get('ELASTICSEARCH_PORT', 9200)
    index_name: str = os.environ.get('ELASTICSEARCH_INDEX_NAME', 'documents')


@dataclass
class CheckpointConfig:
    base_path: str = os.environ.get('CHECKPOINT_BASE_PATH', "s3a://checkpoints")
    enrichment: str = f"{base_path}/event-enrichment"
    aggregations: str = f"{base_path}/hourly-metrics"
    elasticsearch: str = f"{base_path}/elasticsearch"


@dataclass
class Config:
    kafka: KafkaConfig = KafkaConfig()
    iceberg: IcebergConfig = IcebergConfig()
    elasticsearch: ElasticsearchConfig = ElasticsearchConfig()
    checkpoints: CheckpointConfig = CheckpointConfig()


# Global config instance
config = Config()
