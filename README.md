# Kafka Connect E-commerce Analytics

This project is part of a post-graduation course in Data Architecture and Data Engineering by XP Educação. It implements an Azure Blob Storage Sink Connector for Apache Kafka Connect as a practical demonstration of modern data engineering concepts.

## Project Purpose

This project demonstrates the implementation of a real-time data ingestion pipeline using Kafka Connect with Azure Blob Storage integration. The objective is to showcase how streaming data can be efficiently transferred from Kafka topics to cloud storage for analytics and processing.
## Architecture Overview

The project implements a complete Kafka ecosystem with the following components:
- **Kafka Cluster**: 1 broker, 1 zookeeper for distributed coordination
- **Schema Registry**: For schema management and versioning
- **Kafka Connect**: With JDBC and Azure Blob Storage connectors
- **PostgreSQL Database**: Source of e-commerce data (used for demonstration purposes)
- **ksqlDB**: For real-time stream processing
- **Azure Blob Storage**: Destination for streaming data

## Implementation Details

### Connector Components
1. **JDBC Sink Connector** (version 10.9.5)
   - Connects to PostgreSQL database
   - Supports sink operations for relational data

2. **Azure Blob Storage Sink Connector** (version 12.1.9)
   - Transfers Kafka data to Azure Blob Storage
   - Supports JSON format with partitioning
   - Integrates with Confluent's Kafka Connect framework

## Course Objectives

This project fulfills the following learning objectives from XP Educação's Data Architecture and Data Engineering program:

1. **Understanding Streaming Data Architectures**: Demonstrate real-time data flow concepts using Kafka
2. **Cloud Integration**: Implement data ingestion to Azure Blob Storage
3. **Connector Implementation**: Build and deploy custom sink connectors for data movement
4. **Data Pipeline Design**: Create end-to-end data engineering solutions
5. **Modern Data Engineering Practices**: Apply industry-standard tools and frameworks

## Final Results

The completed project demonstrates:

- A fully functional Kafka Connect deployment with multiple connector types
- Real-time streaming of e-commerce events from Kafka topics to Azure Blob Storage
- Secure credential management using environment variables
- Scalable data ingestion architecture that can handle high-volume streams
- Integration between on-premise databases and cloud storage solutions
- Practical application of data engineering principles in a real-world scenario

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Azure Blob Storage account with appropriate permissions
- Valid environment variables configured for Azure credentials

### Setup
1. Clone this repository
2. Configure Azure credentials as environment variables
3. Run `docker-compose up -d` to start all services

### Testing the Connectors

#### 1. Verify Connectors are Installed

