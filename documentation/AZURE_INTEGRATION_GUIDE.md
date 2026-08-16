# Azure Blob Storage Integration Guide

## Current Setup Status

You have successfully created a Kafka topic `test-topic` and verified it's working. The S3 Sink connector is installed (version 12.1.9), but there is no direct Azure Blob Storage connector available in your current installation.

## Limitations

The standard Confluent Kafka Connect distribution includes:
- JDBC Sink Connector (for databases)
- S3 Sink Connector (for AWS S3)

There is no native Azure Blob Storage connector included in the base installation.

## Solutions for Azure Integration

### Option 1: Use S3 Connector with Custom Transformation (Not Recommended for Production)

While not ideal, you could:
1. Configure the S3 Sink connector to write files to a local directory
2. Set up a separate process to move those files to Azure Blob Storage

### Option 2: Install Azure Blob Storage Connector (Recommended)

To use Azure Blob Storage with Kafka Connect, you need to install the Azure Blob Storage connector:

1. **Install from Confluent Hub**:
   ```bash
   docker exec connect confluent-hub install confluentinc/kafka-connect-azure-blob-storage:latest
   ```

2. **Or download manually** from Confluent Hub and copy to the plugins directory.

### Option 3: Use Azure Functions or Event Hubs

Consider using Azure Event Hubs as an intermediary that can then process events into Blob Storage.

## Creating a Sample Configuration (After Installing Azure Connector)

Once the Azure connector is installed, you would create a configuration like:

```json
{
  "name": "azure-blob-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector",
    "tasks.max": "1",
    "topics": "test-topic",
    "azure.blob.storage.account.name": "your-storage-account-name",
    "azure.blob.storage.account.key": "your-storage-account-key",
    "azure.blob.storage.container.name": "ecommerce-analytics",
    "format.class": "io.confluent.connect.azure.blob.format.json.JsonFormat",
    "flush.timeout.ms": "10000",
    "rotate.schedule.interval.ms": "300000",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false"
  }
}
```

## Next Steps

1. **Install Azure Connector** (if not already installed):
   ```bash
   docker exec connect confluent-hub install confluentinc/kafka-connect-azure-blob-storage:latest
   ```

2. **Create the connector configuration** with your Azure credentials

3. **Deploy the connector**:
   ```bash
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @sink/connector_azure_blob_sink.config
   ```

## Alternative Approach

If installing additional connectors is not feasible, consider:

1. Using the S3 Sink connector to write to a local directory
2. Setting up a file synchronization service (like AzCopy) to move files to Azure
3. Using Azure Data Factory or similar services to process the data

## Verification Steps

After installation and configuration:

1. Check if connector is loaded:
   ```bash
   curl http://localhost:8083/connectors
   ```

2. Monitor connector status:
   ```bash
   curl http://localhost:8083/connectors/azure-blob-sink-connector/status
   ```

This setup would enable you to stream your Kafka data directly to Azure Blob Storage for your e-commerce analytics pipeline.