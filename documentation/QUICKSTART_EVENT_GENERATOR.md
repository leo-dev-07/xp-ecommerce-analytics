# Quick Start Guide - E-commerce Event Generator

This guide will help you get the E-commerce Event Generator up and running in minutes.

## Prerequisites

- Docker and Docker Compose installed
- (Optional) Azure Blob Storage account for cloud backup

## Step 1: Configure Environment Variables

Create or update your `.env` file in the project root:

```bash
# PostgreSQL Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=postgres

# Azure Blob Storage (Optional - for cloud backup)
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name
AZURE_STORAGE_ACCOUNT_KEY=your_storage_account_key
AZURE_STORAGE_CONTAINER_NAME=xp-project-ecommece

# Event Generator Configuration
EVENT_GENERATION_INTERVAL=60
```

> **Note**: If you don't have Azure credentials, the system will still work perfectly - it just won't backup data to Azure.

## Step 2: Start All Services

```bash
# Build and start all containers
docker-compose up -d

# This will start:
# - Zookeeper
# - Kafka Broker
# - Schema Registry
# - Kafka Connect
# - PostgreSQL
# - Event Generator (NEW!)
```

## Step 3: Verify Event Generator is Running

```bash
# Check all services are running
docker-compose ps

# View Event Generator logs
docker logs -f event-generator

# You should see output like:
# INFO - Connected to PostgreSQL at postgres:5432
# INFO - All tables verified/created successfully
# INFO - Generating synthetic e-commerce data...
# INFO - Wrote 8 rows to brz_category
# INFO - Wrote 130 rows to brz_brands
# INFO - Wrote 300 rows to brz_customers
# ...
```

Press `Ctrl+C` to stop following the logs.

## Step 4: Verify Data in PostgreSQL

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d postgres

# Check tables
\dt

# View row counts
SELECT 
    'brz_category' as table_name, COUNT(*) as rows FROM brz_category
UNION ALL
SELECT 'brz_brands', COUNT(*) FROM brz_brands
UNION ALL
SELECT 'brz_customers', COUNT(*) FROM brz_customers
UNION ALL
SELECT 'brz_calendar', COUNT(*) FROM brz_calendar
UNION ALL
SELECT 'brz_products', COUNT(*) FROM brz_products
UNION ALL
SELECT 'brz_order_items', COUNT(*) FROM brz_order_items;

# Expected output:
#   table_name      | rows
# ------------------+------
#   brz_category    |    8
#   brz_brands      |  130
#   brz_customers   |  300
#   brz_calendar    | 1096
#   brz_products    |  160
#   brz_order_items | 3000

# View sample data
SELECT * FROM brz_category;
SELECT * FROM brz_order_items LIMIT 10;

# Exit
\q
```

## Step 5: Set Up Kafka Connectors

The connectors read data from PostgreSQL and send it to Kafka topics, then from Kafka to Azure.

```bash
# Make the setup script executable
chmod +x scripts/setup_connectors.sh

# Run the setup script
bash scripts/setup_connectors.sh

# This will create:
# 1. PostgreSQL → Kafka source connector
# 2. Kafka → Azure Blob sink connector (if Azure credentials are set)
```

## Step 6: Verify Kafka Topics

```bash
# List all topics (you should see ecommerce.* topics)
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Expected topics:
# ecommerce.brz_category
# ecommerce.brz_brands
# ecommerce.brz_customers
# ecommerce.brz_calendar
# ecommerce.brz_products
# ecommerce.brz_order_items

# Consume messages from a topic
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_order_items \
  --from-beginning \
  --max-messages 10
```

## Step 7: Verify Complete Setup

Use our verification script to check everything:

```bash
# Make the script executable
chmod +x scripts/verify_setup.sh

# Run verification
bash scripts/verify_setup.sh
```

This will check:
- ✓ All Docker services are running
- ✓ PostgreSQL tables have data
- ✓ Kafka topics exist
- ✓ Connectors are running
- ✓ Event generator is working

## What's Happening?

Here's the complete data flow:

```
Every 60 seconds:

1. Event Generator
   ↓
   Generates synthetic e-commerce data
   ↓
2. PostgreSQL
   ↓
   Stores data in bronze tables (brz_*)
   ↓
3. Kafka JDBC Source Connector
   ↓
   Polls PostgreSQL for new/updated data
   ↓
4. Kafka Topics
   ↓
   Streams data (ecommerce.brz_*)
   ↓
5. Azure Blob Sink Connector
   ↓
   Writes to Azure Blob Storage
   (Time-partitioned: year/month/day/hour)
```

## Common Commands

### View Logs

```bash
# Event Generator
docker logs -f event-generator

# PostgreSQL
docker logs -f postgres

# Kafka Connect
docker logs -f connect

# All services
docker-compose logs -f
```

### Restart Services

```bash
# Restart Event Generator
docker-compose restart event-generator

# Restart all services
docker-compose restart

# Stop all services
docker-compose down

# Start all services
docker-compose up -d
```

### Check Connector Status

```bash
# List all connectors
curl http://localhost:8083/connectors | jq

# Check specific connector status
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status | jq

# View connector configuration
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/config | jq
```

### Database Queries

```bash
# Quick connection
docker exec -it postgres psql -U postgres -d postgres

# Run one-off query
docker exec postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM brz_order_items;"
```

## Troubleshooting

### Event Generator Not Starting

```bash
# Check if PostgreSQL is ready
docker-compose ps postgres

# View Event Generator logs
docker logs event-generator

# Common fix: restart services in order
docker-compose down
docker-compose up -d postgres
sleep 10
docker-compose up -d
```

### No Data in PostgreSQL

```bash
# Check Event Generator is running
docker-compose ps event-generator

# Check logs for errors
docker logs event-generator | grep ERROR

# Manual restart
docker-compose restart event-generator
```

### Connectors Not Working

```bash
# Check Kafka Connect is running
curl http://localhost:8083

# View connector errors
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status | jq

# Restart connector
curl -X POST http://localhost:8083/connectors/postgres-source-ecommerce-all/restart

# Re-run setup script
bash scripts/setup_connectors.sh
```

### No Kafka Topics

This is normal if:
1. Connectors haven't been set up yet → Run `bash scripts/setup_connectors.sh`
2. No data in PostgreSQL yet → Wait for Event Generator to run (every 60s)
3. Connector not polling yet → Wait a few seconds for the connector to start

## Configuration Options

### Change Data Generation Frequency

Edit `.env`:
```bash
EVENT_GENERATION_INTERVAL=30  # Generate every 30 seconds
```

Then restart:
```bash
docker-compose restart event-generator
```

### Generate More Data

Edit `scripts/event_generator_postgres_azure.py` (around line 580):

```python
brands_rows = generate_brands_table(num_brands=200)  # More brands
customers_rows = generate_customers_table(num_customers=500)  # More customers
products_rows = generate_products_table(brands_rows, products_per_category=30)
order_items_rows = generate_order_items_table(
    customers_rows, products_rows, calendar_rows, num_order_items=5000
)
```

Then rebuild:
```bash
docker-compose up -d --build event-generator
```

## Next Steps

1. **Explore the Data**: Query PostgreSQL to understand the schema
2. **Monitor Kafka**: Watch messages flowing through topics
3. **Check Azure**: Verify data is being backed up to Azure Blob Storage
4. **Build Analytics**: Use the data for your analytics pipeline
5. **Customize**: Modify the generator to match your specific needs

## Additional Resources

- Detailed documentation: `scripts/README_EVENT_GENERATOR.md`
- Connector configs: `connectors/source/` and `connectors/sink/`
- Database schema: `postgres/init.sql`
- Event generator code: `scripts/event_generator_postgres_azure.py`

## Getting Help

If you encounter issues:

1. Run the verification script: `bash scripts/verify_setup.sh`
2. Check logs: `docker-compose logs`
3. Review the detailed README: `scripts/README_EVENT_GENERATOR.md`
4. Check PostgreSQL: `docker exec -it postgres psql -U postgres -d postgres`

## Success Criteria

You know everything is working when:

- ✅ All 6 Docker containers are running
- ✅ PostgreSQL has 6 bronze tables with data
- ✅ 6 Kafka topics exist (ecommerce.brz_*)
- ✅ Connectors show "RUNNING" status
- ✅ New data appears every 60 seconds
- ✅ (Optional) Data appears in Azure Blob Storage

Congratulations! Your E-commerce Event Generator is now running! 🎉
