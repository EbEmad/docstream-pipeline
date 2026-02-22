from pyspark.sql import SparkSession
from typing import Optional
import logging

from .config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def create_spark_session(app_name:Optional[str]=None)->SparkSession:
    """
    Create Spark session - all configs loaded from spark-defaults.conf
    """

    logger.info(f"Creating Spark session: {app_name or 'DocStream Pipeline'}")

    builder=SparkSession.builder
    if app_name:
        builder=builder.appName(app_name)
    spark=builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    logger.info("Spark session created")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Master: {spark.sparkContext.master}")
    logger.info(f"Catalog: {config.iceberg.catalog_name}")

    return spark

def stop_spark_session(spark:SparkSession)->None:
    """Stop Spark session."""
    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("Spark session stopped.")