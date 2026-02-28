from pyspark.sql.functions import (
    col, window, count, avg, when, 
    date_format, hour, current_timestamp
)

from common.spark_session import create_spark_session, stop_spark_session
from common.transformations import add_columns
from common.data_quality import validate_aggregated_metrics
from common.config import config
from common.logger import setup_logging

logger = setup_logging("RealtimeAggregations")

class RealtimeAggregations:
    """Compute real-time aggregations from enriched events"""

    def __init__(self):
        self.spark=create_spark_session("RealtimeAggregations")
        self._ensure_database()

        self.source_table = f"{config.iceberg.catalog_name}.{config.iceberg.namespace_events}.{config.iceberg.table_documents}"
        self.target_table = f"{config.iceberg.catalog_name}.{config.iceberg.namespace_metrics}.{config.iceberg.table_metrics}"

        logger.info(f"Source table: {self.source_table}")
        logger.info(f"Target table: {self.target_table}")

    
    def _ensure_database(self):
        """Ensure metrics database exists"""       
        logger.info(f"Ensuring database exists: {config.iceberg.namespace_metrics}")

        self.spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {config.iceberg.catalog_name}.{config.iceberg.namespace_metrics}
        """)

        logger.info("Database ready")
    
    def read_enriched_stream(self):
        """Read enriched events from Iceberg"""
        logger.info(f"Reading enriched events from {self.source_table}...")

        enriched_stream = self.spark.readStream \
            .format("iceberg") \
            .table(self.source_table)
        logger.info("Enriched stream created")
        return enriched_stream


    def compute_metrics(self, enriched_df):
        """Compute hourly aggregations by category"""
        logger.info("Computing aggregations...")

        watermarked = enriched_df.withWatermark("updated_at", "10 minutes")

        metrics = watermarked \
            .groupBy(
                window(col("updated_at"), "1 hour"),
                col("template_category")
            ) \
            .agg(
                count(when(col("status") == "draft", 1)).alias("documents_created"),
                count(when(col("status") == "sent", 1)).alias("documents_sent"),
                count(when(col("status") == "viewed", 1)).alias("documents_viewed"),
                count(when(col("status") == "signed", 1)).alias("documents_signed"),
                count(when(col("status") == "completed", 1)).alias("documents_completed"),

                avg("time_to_view_minutes").alias("avg_time_to_view_minutes"),
                avg("time_to_sign_minutes").alias("avg_time_to_sign_minutes"),
                avg("time_to_complete_minutes").alias("avg_time_to_complete_minutes"),
                
                count("*").alias("total_updates")
            )
        
        conversions = [
            ("sent_to_viewed_rate", when(col("documents_sent") > 0, col("documents_viewed") / col("documents_sent")).otherwise(0.0)),
            
            ("viewed_to_signed_rate", when(col("documents_viewed") > 0, col("documents_signed") / col("documents_viewed")).otherwise(0.0)),
            
            ("overall_completion_rate", when(col("documents_sent") > 0, col("documents_completed") / col("documents_sent")).otherwise(0.0)),
            
            ("window_start", col("window.start")),
            ("window_end", col("window.end")),
            ("metric_date", date_format(col("window.start"), "yyyy-MM-dd")),
            ("metric_hour", hour(col("window.start"))),
            ("processing_timestamp", current_timestamp())
        ]


        final_metrics=add_columns(metrics,conversions)

        result=final_metrics.select(
            "metric_date", "metric_hour", "window_start", "window_end",
            "template_category",
            "documents_created", "documents_sent", "documents_viewed",
            "documents_signed", "documents_completed",
            "sent_to_viewed_rate", "viewed_to_signed_rate", "overall_completion_rate",
            "avg_time_to_view_minutes", "avg_time_to_sign_minutes",
            "avg_time_to_complete_minutes",
            "total_updates", "processing_timestamp"
        )

        logger.info("Aggregations computed")
        return result


    def run(self):
        """Run the aggregations pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Real-Time Aggregations Job")
        logger.info("=" * 60)

        try:
            enriched_stream = self.read_enriched_stream()
            metrics_stream = self.compute_metrics(enriched_stream)
            validated_stream = validate_aggregated_metrics(metrics_stream)

            logger.info(f"Writing metrics to {self.target_table}...")

            query = validated_stream \
                .writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("checkpointLocation", config.checkpoints.aggregations) \
                .trigger(processingTime="1 minute") \
                .toTable(self.target_table)
            
            logger.info(f"Pipeline running - writing to {self.target_table}")
            logger.info(f"Checkpoint: {config.checkpoints.aggregations}")
            query.awaitTermination()


        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(self.spark)



def main():
    job=RealtimeAggregations()
    job.run()

if __name__=="__main__":
    main()