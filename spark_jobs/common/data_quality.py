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