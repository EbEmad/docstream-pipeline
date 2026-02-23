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
from common.logger import setup_logging

logger = setup_logging("EventEnrichment")

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

    def read_cdc_stream(self):
        """Read CDC events from Kafka with Avro deserialization"""
        logger.info("Reading CDC stream from Kafka...")

        # Get Avro schema
        schema_url = f"{config.kafka.schema_registry_url}/subjects/{self.topic}-value/versions/latest"
        response = requests.get(schema_url)
        response.raise_for_status()
        schema_json=response.json()['schema']

        kafka_df=self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Deserialize Avro skip 5-byte Confluent header
        cdc_df=kafka_df.select(
            from_avro(
                expr("substring(value, 6, length(value)-5)"),
                schema_json
            ).alias("data")
        ).select("data.*")

        logger.info("CDC stream created")
        return cdc_df
    def transform_events(self, cdc_df):
        """Apply transformations: time deltas, business logic, temporal fields"""
        logger.info("Applying transformations...")

        df=cdc_df \
            .withColumn("created_at",col("created_at").cast("timestamp")) \
            .withColumn("sent_at", col("sent_at").cast("timestamp")) \
            .withColumn("viewed_at", col("viewed_at").cast("timestamp")) \
            .withColumn("signed_at", col("signed_at").cast("timestamp")) \
            .withColumn("completed_at", col("completed_at").cast("timestamp")) \
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
        
        transformations=[
            ("time_to_send_minutes",
                when(col("sent_at").isNotNull(),
                     (unix_timestamp("sent_at") - unix_timestamp("created_at")) / 60.0
                ).otherwise(None)
            ),
            ("time_to_view_minutes",
             when(col("signed_at").isNotNull() & col("sent_at").isNotNull(),
                (unix_timestamp("viewed_at")-unix_timestamp("sent_at"))/60.0
                ).otherwise(None)
            ),
            ("time_to_sign_minutes",
                when(col("signed_at").isNotNull() & col("sent_at").isNotNull(),
                     (unix_timestamp("signed_at") - unix_timestamp("sent_at")) / 60.0
                ).otherwise(None)
            ),
            ("time_to_complete_minutes",
                when(col("completed_at").isNotNull() & col("sent_at").isNotNull(),
                     (unix_timestamp("completed_at") - unix_timestamp("sent_at")) / 60.0
                ).otherwise(None)
            ),

            ("event_date",date_format("updated_at", "yyyy-MM-dd")),
            ("event_hour",hour("updated_at")),
            ("is_business_hours",
                when((hour("updated_at") >= 8) & (hour("updated_at") < 18), lit(True))
                .otherwise(lit(False))
            ),
            ("processing_timestamp", current_timestamp())
        ]

        transfromed=add_columns(df,transformations)
        return transfromed


    def run(self):
        """Run the enrichment pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Event Enrichment Job")
        logger.info("=" * 60)

        try:
            # Read CDC stream
            cdc_stream = self.read_cdc_stream()

            # Apply transformations
            enriched_stream = self.transform_events(cdc_stream)

            logger.info(f"Writing enriched events to {self.target_table}...")

            validated_stream=validate_enriched_events(enriched_stream)

            query=validated_stream \
                    .wrtiteStream \
                    .format("iceberg") \
                    .outputMode("append") \
                    .trigger(processingTime="30 seconds") \
                    .option("maxOffsetsPerTrigger", "1000") \
                    .option("checkpointLocation", config.checkpoints.enrichment) \
                    .option("fanout-enabled", "true") \
                    .partitionBy("event_date") \
                    .toTable(self.target_table) 
            
            logger.info(f"Pipeline running - writing to {self.target_table}")
            logger.info(f"Checkpoint: {config.checkpoints.enrichment}")
            query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(self.spark)


def main():
    job = EventEnrichment()
    job.run()

if __name__ == "__main__":
    main()