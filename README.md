# DocStream Real-Time Data Pipeline

[![Architecture](https://img.shields.io/badge/Architecture-Streaming-blue)]() [![Kafka](https://img.shields.io/badge/Kafka-3%20Node%20Cluster-orange)]() [![Spark](https://img.shields.io/badge/Spark-Streaming-red)]() [![Iceberg](https://img.shields.io/badge/Iceberg-1.4.0-green)]() [![Elasticsearch](https://img.shields.io/badge/Elasticsearch-8.11.0-blueviolet)]()

## Overview

A real-time data pipeline that captures Change Data Capture (CDC) events from PostgreSQL, processes them through Apache Spark Structured Streaming, and stores results in Apache Iceberg tables with real-time indexing to Elasticsearch.



```mermaid
graph TD
    subgraph "Data Generation"
        P[Producer Service] -->|SQL Inserts| PG[(PostgreSQL)]
    end

    subgraph "Change Data Capture (CDC)"
        PG -.->|WAL Logs| KC[Kafka Connect / Debezium]
        KC -->|Register Schema| SR[Schema Registry]
        KC -->|Publish Events| K[(Kafka Cluster)]
    end

    subgraph "Stream Processing"
        K -->|Read Stream| SE[Spark Enrichment Job]
        SR <-->|Fetch Schema| SE
        SE -->|Enriched Events| REST[Iceberg REST Catalog]
        SE -->|Write Parquet| MINIO[(MinIO S3 Storage)]
    end

    subgraph "Real-time Analytics"
        REST <-->|Metadata| RA[Real-time Aggregation Job]
        MINIO -->|Read Enriched| RA
        RA -->|Write Metrics| REST
        RA -->|Store Aggregates| MINIO
    end

    subgraph "Search & Visualization"
        REST <-->|Read Table| EI[Elasticsearch Indexer Job]
        EI -->|Push Docs| ES[(Elasticsearch)]
        ES <-->|Query| KB[Kibana]
    end
```

## System Services

| Service | URL | Description |
| :--- | :--- | :--- |
| **Kafka UI** | [http://localhost:8082](http://localhost:8082) | Monitor Kafka topics, consumers, and connectors |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | Monitor Spark jobs and worker status |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | S3 object storage (User: `admin`, Pass: `password`) |
| **Kibana** | [http://localhost:5601](http://localhost:5601) | Visualize data in Elasticsearch |
| **Elasticsearch** | [http://localhost:9200](http://localhost:9200) | REST API for search and analytics |
| **Iceberg REST** | [http://localhost:8181](http://localhost:8181) | Iceberg catalog management |