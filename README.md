# rag-pipeline

## Overview

This project implements a data pipeline designed to handle data ingestion, transformation, storage, and monitoring. The pipeline uses a variety of tools and technologies to provide a robust, scalable, and efficient solution for managing data from ingestion to vector storage. The core components include Apache Airflow, PostgreSQL, Debezium, Kafka, a custom embedding service, Qdrant, and monitoring with Prometheus and Grafana.

## Architecture

The architecture consists of the following components:

- **Apache Airflow**: Orchestrates and schedules workflows for data ingestion and processing.
- **PostgreSQL**: Serves as the primary database for storing ingested data.
- **Debezium**: Captures changes from PostgreSQL and streams them to Kafka.
- **Kafka**: A distributed message broker that facilitates data streaming.
- **Embedding Service**: Consumes Kafka messages, processes the data, and generates embeddings using machine learning models.
- **Qdrant**: A vector database optimized for storing and searching high-dimensional vectors.
- **Monitoring**: Prometheus and Grafana for monitoring and visualization of system metrics.
