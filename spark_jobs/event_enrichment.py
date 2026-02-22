from pyspark.sql.functions import (
    col, expr, when, unix_timestamp, current_timestamp, 
    lit, date_format, hour
)
from pyspark.sql.avro.functions import from_avro
import requests
from common.spark_session import create_spark_session, stop_spark_session
