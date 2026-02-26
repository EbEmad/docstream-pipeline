"""
Data Quality Validation Module
Simple validation rules for streaming data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logger = logging.getLogger(__name__)

def validate_enriched_events(df: DataFrame) -> DataFrame:
    """
    Validate enriched events before writing to Iceberg
    
    Rules:
    - document_id must not be null
    - status must not be null
    - updated_at must not be null
    - template_category must not be null
    
    Returns:
        DataFrame with only valid records
    """
    logger.info("Applying validation rules to enriched events...")
    
    validated_df = df.filter(
        col("document_id").isNotNull() &
        col("status").isNotNull() &
        col("updated_at").isNotNull() &
        col("template_category").isNotNull()
    )
    
    logger.info("Validation rules applied")
    return validated_df

def validate_aggregated_metrics(df: DataFrame) -> DataFrame:
    """
    Validate aggregated metrics before writing
    
    Rules:
    - window_start and window_end must not be null
    - template_category must not be null
    - counts must be >= 0
    - rates must be between 0 and 1
    
    Returns:
        DataFrame with only valid records
    """

    logger.info("Applying validation rules to aggregated metrics...")

    validated_df = df.filter(
        col("window_start").isNotNull() &
        col("window_end").isNotNull() &
        col("template_category").isNotNull() &
        (col("documents_created") >= 0) &
        (col("documents_sent") >= 0) &
        (col("documents_viewed") >= 0) &
        (col("documents_signed") >= 0) &
        (col("documents_completed") >= 0) &
        (col("total_updates") >= 0) &
        col("sent_to_viewed_rate").between(0, 1) &
        col("viewed_to_signed_rate").between(0, 1) &
        col("overall_completion_rate").between(0, 1)
    )

    logger.info("Validation rules applied")
    return validated_df