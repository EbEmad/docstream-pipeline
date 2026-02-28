from common.spark_session import create_spark_session,stop_spark_session
from common.transformations import convert_timestamps_to_iso
from common.config import config
from common.logger import setup_logging


logger = setup_logging("ElasticsearchIndexer")



class ElasticsearchIndexer:
    """Index enriched events to Elasticsearch"""


    def __init__(self):
        self.spark=create_spark_session("ElasticsearchIndexer")

        self.source_table = f"{config.iceberg.catalog_name}.{config.iceberg.namespace_events}.{config.iceberg.table_documents}"
        self.elasticsearch_index = config.elasticsearch.index_name

        logger.info(f"Source table: {self.source_table}")
        logger.info(f"Elasticsearch index: {self.elasticsearch_index}")
        logger.info(f"Elasticsearch host: {config.elasticsearch.host}:{config.elasticsearch.port}")


    def read_enriched_stream(self):
        """Read enriched events from Iceberg"""
        logger.info(f"Reading enriched events from {self.source_table}...")

        enriched_stream = self.spark.readStream \
            .format("iceberg") \
            .table(self.source_table)
        
        logger.info("Enriched stream created")
        return enriched_stream
    
    def write_batch_to_elasticsearch(self, batch_df, batch_id):
        """Write a batch to Elasticsearch"""

        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty, skipping")
            return
        
        try:
            batch_df.write \
                    .format("es")\
                    .option("es.nodes", config.elasticsearch.host) \
                    .option("es.port", str(config.elasticsearch.port)) \
                    .option("es.mapping.id", "document_id") \
                    .option("es.batch.size.entries", "500") \
                    .option("es.batch.size.bytes", "5mb") \
                    .option("es.write.operation", "upsert") \
                    .mode("append") \
                    .sae(self.elasticsearch_index)
            
            count=batch_df.count()
            logger.info(f"Batch {batch_id}: Indexed {count} documents to Elasticsearch")
        except Exception as e:
            logger.error(f"Batch {batch_id} failed: {e}", exc_info=True)

    

    def prepare_for_elasticsearch(self,df):
        """Prepare data for Elasticsearch indexing"""
        logger.info("Preparing data for Elasticsearch...")

        selected=df.select(
            "document_id", "title", "status", "template_category",
            "created_at", "sent_at", "viewed_at", "signed_at", "completed_at",
            "time_to_view_minutes", "time_to_sign_minutes", "time_to_complete_minutes",
            "event_date", "event_hour", "is_business_hours", "processing_timestamp"
        )


        timestamp_cols=[
            "created_at", "sent_at", "viewed_at", 
            "signed_at", "completed_at", "processing_timestamp"
        ]

        elasticsearch_ready = convert_timestamps_to_iso(selected, timestamp_cols)
        logger.info("Data prepared for Elasticsearch")
        return elasticsearch_ready


    def run(self):
        """Run the Elasticsearch indexing pipeline"""
        logger.info("=" * 60)
        logger.info("Starting Elasticsearch Indexer Job")
        logger.info("=" * 60)


        try:
            enriched_stream = self.read_enriched_stream()

            elasticsearch_stream = self.prepare_for_elasticsearch(enriched_stream)

            logger.info(f"Starting Elasticsearch indexing to index: {self.elasticsearch_index}...")

            query=elasticsearch_stream \
                .writeStream \
                .foreachBatch(self.write_batch_to_elasticsearch) \
                .option("checkpointLocation",config.checkpoints.elasticsearch) \
                .trigger(processingTime="30 seconds") \
                .option("maxRecordsPerBatch", "1000") \
                .start()
            
            logger.info(f"Pipeline running - indexing to {self.elasticsearch_index}")
            logger.info(f"Checkpoint: {config.checkpoints.elasticsearch}")
            query.awaitTermination()
            
    

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(self.spark)
    


def main():
    job=ElasticsearchIndexer()
    job.run()

if __name__=="__main__":
    main()