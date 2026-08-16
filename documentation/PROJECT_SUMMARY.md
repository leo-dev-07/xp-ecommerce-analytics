# Azure Blob Storage Sink Connector Project

## Project Overview

This project implements an Azure Blob Storage Sink Connector for Apache Kafka Connect. The connector enables streaming data from Kafka topics directly into Azure Blob Storage, facilitating real-time data ingestion and analytics pipelines.

## Architecture

The connector is built using the Confluent Kafka Connect framework and integrates with:

- **Kafka Connect**: For managing data flows
- **Azure Blob Storage**: For scalable data storage
- **Kafka Topics**: As the source of streaming data

## Key Features

- **Sink Connector Implementation**: Transfers Kafka records to Azure Blob Storage
- **JSON Format Support**: Stores data in JSON format for easy processing
- **Partitioning**: Automatically partitions data by time and topic
- **Configuration Management**: Uses environment variables for secure credential handling
- **Scheduled Rotation**: Rotates files at regular intervals (default 5 minutes)

## Configuration

The connector configuration includes:

- **Azure Credentials**: Account name, account key, and container name (using environment variables)
- **Topic Selection**: Specifies which Kafka topics to consume
- **Format Settings**: JSON format with schema support
- **Rotation Settings**: File rotation interval (300 seconds by default)
- **Performance Tuning**: Flush size configuration for batch processing

## Installation Process

### Prerequisites
1. Docker and Docker Compose installed
2. Access to Azure Blob Storage account
3. Kafka Connect cluster running

### Installation Steps
1. Install the Azure Blob Storage connector from Confluent Hub:
   ```bash
   docker exec connect confluent-hub install confluentinc/kafka-connect-azure-blob-storage:latest
   ```

2. Configure environment variables for Azure credentials:
   - `AZURE_STORAGE_ACCOUNT_NAME`
   - `AZURE_STORAGE_ACCOUNT_KEY` 
   - `AZURE_STORAGE_CONTAINER_NAME`

3. Deploy the connector configuration using REST API or Confluent Control Center

## Usage Example

After installation, create a connector configuration like:

```json
{
  "name": "azure-blob-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector",
    "tasks.max": "1",
    "topics": "ecommerce_events",
    "format.class": "io.confluent.connect.azure.blob.format.json.JsonFormat",
    "storage.class": "io.confluent.connect.azure.blob.storage.AzureBlobStorage",
    "azblob.account.name": "${env:AZURE_STORAGE_ACCOUNT_NAME}",
    "azblob.account.key": "${env:AZURE_STORAGE_ACCOUNT_KEY}",
    "azblob.container.name": "${env:AZURE_STORAGE_CONTAINER_NAME}",
    "flush.size": "1000",
    "rotate.schedule.interval.ms": "300000"
  }
}
```

## Integration Points

- **Kafka Topics**: Data sources for streaming
- **Azure Blob Storage**: Destination for persistent storage
- **Confluent Hub**: Connector distribution platform
- **Docker Environment**: Deployment and orchestration

## Benefits

1. **Real-time Data Ingestion**: Stream data from Kafka to Azure Blob Storage
2. **Scalability**: Handles large volumes of streaming data
3. **Cost-effective**: Leverages Azure Blob Storage's pay-as-you-go pricing
4. **Integration Ready**: Works seamlessly with existing Kafka ecosystem
5. **Flexible Format**: Supports JSON format for easy processing

## Deployment Requirements

- Running Kafka Connect cluster
- Valid Azure Blob Storage credentials
- Proper network connectivity to Azure services
- Appropriate resource allocation for connector tasks