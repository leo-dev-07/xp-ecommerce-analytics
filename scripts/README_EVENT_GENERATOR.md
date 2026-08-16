# E-commerce Event Generator

## Overview

The E-commerce Event Generator is a containerized service that continuously generates synthetic e-commerce data and writes it to both PostgreSQL (which feeds Kafka via JDBC connector) and Azure Blob Storage for backup and analytics.

## Architecture

```
┌─────────────────────────┐
│  Event Generator        │
│  (Docker Container)     │
│                         │
│  - Generates synthetic  │
│    e-commerce data      │
│  - Runs every 60s       │
└───────────┬─────────────┘
            │
            ├──────────────────┐
            │                  │
            ▼                  ▼
┌───────────────────┐  ┌──────────────────┐
│   PostgreSQL      │  │  Azure Blob      │
│   Database        │  │  Storage         │
│                   │  │                  │
│  Bronze Tables:   │  │  landing-zone/   │
│  - brz_category   │  │  - category/     │
│  - brz_brands     │  │  - brands/       │
│  - brz_customers  │  │  - customers/    │
│  - brz_calendar   │  │  - calendar/     │
│  - brz_products   │  │  - products/     │
│  - brz_order_items│  │  - order_items/  │
└─────────┬─────────┘  └──────────────────┘
          │
          │ (JDBC Connector)
          ▼
┌───────────────────┐
│  Kafka Topics     │
│                   │
│  - ecommerce.     │
│    brz_category   │
│  - ecommerce.     │
│    brz_brands     │
│  - etc.           │
└───────────────────┘
```

## Tables Generated

### Dimension Tables

1. **brz_category** - Product categories (8 rows)
   - category_code (PK)
   - category_name
   - dt_update

2. **brz_brands** - Product brands (130 rows)
   - brand_code (PK)
   - brand_name
   - category_code (FK)
   - dt_update

3. **brz_customers** - Customer information (300 rows)
   - customer_id (PK)
   - phone
   - country_code
   - country
   - state
   - city
   - dt_update

4. **brz_calendar** - Date dimension (1096 days: 2024-2026)
   - date_code (PK)
   - full_date
   - year, quarter, month, month_name
   - day, day_of_week, day_name
   - is_weekend
   - dt_update

5. **brz_products** - Product catalog (160 rows)
   - product_id (PK)
   - product_name
   - brand_code (FK)
   - category_code (FK)
   - price
   - description
   - dt_update

### Fact Table

6. **brz_order_items** - Order line items (3000 rows)
   - order_item_id (PK)
   - order_id
   - customer_id (FK)
   - product_id (FK)
   - date_code (FK)
   - quantity
   - unit_price
   - total_price
   - dt_update

## Environment Variables

### Required (PostgreSQL)

- `POSTGRES_HOST` - PostgreSQL host (default: `postgres`)
- `POSTGRES_PORT` - PostgreSQL port (default: `5432`)
- `POSTGRES_DB` - Database name (default: `postgres`)
- `POSTGRES_USER` - Database user (default: `postgres`)
- `POSTGRES_PASSWORD` - Database password (default: `password`)

### Optional (Azure)

- `AZURE_STORAGE_ACCOUNT_NAME` - Azure Storage account name
- `AZURE_STORAGE_ACCOUNT_KEY` - Azure Storage account key
- `AZURE_STORAGE_CONTAINER_NAME` - Container name (default: `xp-project-ecommece`)

### Configuration

- `EVENT_GENERATION_INTERVAL` - Seconds between data generation cycles (default: `60`)

## Quick Start

### 1. Prerequisites

Ensure you have a `.env` file in the project root with the required credentials:

```bash
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=postgres

# Azure (optional)
AZURE_STORAGE_ACCOUNT_NAME=your_account_name
AZURE_STORAGE_ACCOUNT_KEY=your_account_key
AZURE_STORAGE_CONTAINER_NAME=xp-project-ecommece

# Event Generator
EVENT_GENERATION_INTERVAL=60
```

### 2. Start All Services

```bash
# Build and start all containers
docker-compose up -d

# View event generator logs
docker logs -f event-generator

# Check all services
docker-compose ps
```

### 3. Verify Data Generation

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d postgres

# Check table row counts
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

# Exit psql
\q
```

### 4. Set Up Kafka Connectors

#### Source Connector (PostgreSQL → Kafka)

```bash
# Create JDBC Source Connector for all tables
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/source/connector_brz_all_tables.json

# Check connector status
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status
```

#### Sink Connector (Kafka → Azure Blob)

```bash
# Create Azure Blob Sink Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/sink/connector_azure_ecommerce_all.json

# Check connector status
curl http://localhost:8083/connectors/azure-blob-sink-ecommerce-all/status
```

### 5. Verify Kafka Topics

```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Expected topics:
# - ecommerce.brz_category
# - ecommerce.brz_brands
# - ecommerce.brz_customers
# - ecommerce.brz_calendar
# - ecommerce.brz_products
# - ecommerce.brz_order_items

# Consume messages from a topic
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_order_items \
  --from-beginning \
  --max-messages 10
```

## Data Flow

1. **Event Generator** generates synthetic e-commerce data every 60 seconds
2. **PostgreSQL** stores the data in bronze layer tables with `dt_update` timestamp
3. **Kafka JDBC Source Connector** polls PostgreSQL tables based on `dt_update` column
4. **Kafka Topics** receive new/updated records from PostgreSQL
5. **Azure Blob Sink Connector** writes Kafka messages to Azure Blob Storage with time-based partitioning
6. **Azure Blob Storage** stores files in `kafka-topics/` directory with hourly partitions

## Monitoring

### Check Event Generator Health

```bash
# View real-time logs
docker logs -f event-generator

# Check last 100 lines
docker logs --tail 100 event-generator

# Restart if needed
docker-compose restart event-generator
```

### Check Database Connections

```bash
# Check PostgreSQL connections
docker exec postgres psql -U postgres -d postgres -c "SELECT * FROM pg_stat_activity WHERE datname = 'postgres';"
```

### Check Kafka Connect

```bash
# List all connectors
curl http://localhost:8083/connectors

# Check specific connector
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status | jq

# View connector config
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/config | jq
```

## Troubleshooting

### Event Generator Not Starting

```bash
# Check logs
docker logs event-generator

# Common issues:
# 1. PostgreSQL not ready - wait for healthcheck
# 2. Missing environment variables - check .env file
# 3. Azure credentials invalid - Azure writes will fail but PostgreSQL continues
```

### No Data in PostgreSQL

```bash
# Connect to database
docker exec -it postgres psql -U postgres -d postgres

# Check if tables exist
\dt

# Check table schemas
\d brz_category

# Check for errors in event generator logs
docker logs event-generator | grep ERROR
```

### Kafka Connector Not Working

```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status

# Restart connector
curl -X POST http://localhost:8083/connectors/postgres-source-ecommerce-all/restart

# Delete and recreate connector
curl -X DELETE http://localhost:8083/connectors/postgres-source-ecommerce-all
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connectors/source/connector_brz_all_tables.json
```

### No Data in Kafka Topics

```bash
# Check if topics exist
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Check topic details
docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe --topic ecommerce.brz_category

# Try consuming from beginning
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_category \
  --from-beginning
```

## Configuration Files

- `scripts/event_generator_postgres_azure.py` - Main event generator script
- `scripts/Dockerfile` - Container definition for event generator
- `scripts/requirements.txt` - Python dependencies
- `scripts/azure_blob_writer.py` - Azure Blob Storage writer helper
- `postgres/init.sql` - PostgreSQL schema initialization
- `connectors/source/connector_brz_all_tables.json` - JDBC source connector config
- `connectors/sink/connector_azure_ecommerce_all.json` - Azure sink connector config

## Customization

### Change Generation Interval

Set `EVENT_GENERATION_INTERVAL` in `.env`:

```bash
EVENT_GENERATION_INTERVAL=30  # Generate data every 30 seconds
```

### Adjust Data Volume

Edit `scripts/event_generator_postgres_azure.py`:

```python
# Line ~580-590
brands_rows = generate_brands_table(num_brands=200)  # More brands
customers_rows = generate_customers_table(num_customers=500)  # More customers
products_rows = generate_products_table(brands_rows, products_per_category=30)  # More products
order_items_rows = generate_order_items_table(
    customers_rows, products_rows, calendar_rows, num_order_items=5000  # More orders
)
```

### Disable Azure Backup

Simply don't set Azure credentials in `.env` - the generator will only write to PostgreSQL.

## Clean Up

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove only event generator
docker-compose stop event-generator
docker-compose rm -f event-generator
```

## Performance Notes

- Default configuration generates ~5000 total rows every 60 seconds
- PostgreSQL handles updates efficiently using ON CONFLICT
- Kafka Connect polls every 5 seconds for new data
- Azure Blob files rotate every 5 minutes (300000ms)
- Time-based partitioning creates hourly folders in Azure

## Support

For issues or questions, check:
1. Event generator logs: `docker logs event-generator`
2. PostgreSQL logs: `docker logs postgres`
3. Kafka Connect logs: `docker logs connect`
4. This README and inline code documentation
