from pyspark.sql.functions import (
    col, expr, when, unix_timestamp, current_timestamp, 
    lit, date_format, hour
)
from pyspark.sql.avro.functions import from_avro
import requests
from common.spark_session import create_spark_session, stop_spark_session
from common.transformations import add_columns
from common.data_quality import validate_enriched_events
from common.config import config
from common.logger import setup_logger

logger=setup_logger("EventEnrichment")

class EventEnrichment:
    """Enrich CDC events and write to Iceberg"""

    def __init__(self):
        self.spark=create_spark_session("EventEnrichment")
        self._ensure_database()

        self.topic=f"{config.kafka.topic_prefix}.public.documents"
        self.target_table=f"{config.iceberg.catalog_name}.{config.iceberg.namespace_events}.{config.iceberg.table_documents}"
        
        logger.info(f"Kafka servers: {config.kafka.bootstrap_servers}")
        logger.info(f"Schema registry: {config.kafka.schema_registry_url}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Target table: {self.target_table}")
        
    
    def _ensure_database(self):
        """Ensure events database exists"""
        logger.info(f"Ensuring database exists: {config.iceberg.namespace_events}")
        self.spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog_name}.{config.iceberg.namespace_events}
            """
        )
        logger.info("Database ready")

def main():
    pass

if __name__ == "__main__":
    main()