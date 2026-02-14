# DocStream Real-Time Data Pipeline

[![Architecture](https://img.shields.io/badge/Architecture-Streaming-blue)]() [![Kafka](https://img.shields.io/badge/Kafka-3%20Node%20Cluster-orange)]() [![Spark](https://img.shields.io/badge/Spark-Streaming-red)]() [![Iceberg](https://img.shields.io/badge/Iceberg-1.4.0-green)]() [![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11.0-blueviolet)]()

## Overview

A real-time data pipeline that captures Change Data Capture (CDC) events from PostgreSQL, processes them through Apache Spark Structured Streaming, and stores results in Apache Iceberg tables with real-time indexing to Elasticsearch.