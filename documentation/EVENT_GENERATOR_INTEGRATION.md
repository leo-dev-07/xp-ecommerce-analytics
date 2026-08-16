# Event Generator Integration Guide

## Overview

This document describes the E-commerce Event Generator integration into the Kafka Connect analytics pipeline. The Event Generator is a containerized service that continuously generates realistic synthetic e-commerce data for testing and demonstration purposes.

## Architecture Integration

### Before Event Generator

```
Manual Data → PostgreSQL → Kafka Connect → Kafka Topics → Azure Blob Storage
```

### With Event Generator

```
┌──────────────────────┐
│  Event Generator     │ ← Automated, runs every 60s
│  (Container)         │
└─────────┬────────────┘
          │
          ├─────────────────────┐
          ▼                     ▼
  ┌──────────────┐    ┌──────────────────┐
  │  PostgreSQL  │    │  Azure Blob      │
  │  (Bronze)    │    │  Storage (Backup)│
  └──────┬───────┘    └──────────────────┘
         │
         │ (JDBC Source Connector)
         ▼
  ┌──────────────┐
  │ Kafka Topics │
  │ ecommerce.*  │
  └──────┬───────┘
         │
         │ (Azure Sink Connector)
         ▼
  ┌──────────────────┐
  │  Azure Blob      │
  │  Storage (Kafka) │
  └──────────────────┘
```

## What Was Added

### New Components

1. **Event Generator Container** (`event-generator`)
   - Python-based data generator
   - Runs continuously with configurable interval (default: 60 seconds)
   - Generates 6 tables with realistic e-commerce data
   - Writes to both PostgreSQL and Azure (optional)

2. **Bronze Layer Schema** (PostgreSQL)
   - `brz_category` - 8 product categories
   - `brz_brands` - 130 fictional brands
   - `brz_customers` - 300 customers across 35 cities/countries
   - `brz_calendar` - 1,096 days (2024-2026)
   - `brz_products` - 160 products with descriptions
   - `brz_order_items` - 3,000 order line items

3. **Kafka Source Connector**
   - Polls PostgreSQL tables every 5 seconds
   - Creates topic per table: `ecommerce.brz_*`
   - Uses timestamp-based incremental mode

4. **Azure Sink Connector**
   - Streams Kafka messages to Azure Blob Storage
   - Time-based partitioning (hourly)
   - JSON format

### New Files

```
scripts/
├── event_generator_postgres_azure.py  ← Main generator script
├── azure_blob_writer.py                ← Azure helper (existing)
├── Dockerfile                          ← Container definition
├── requirements.txt                    ← Python dependencies
├── setup_connectors.sh                 ← Connector setup automation
├── verify_setup.sh                     ← Health check script
└── README_EVENT_GENERATOR.md           ← Detailed documentation

connectors/
├── source/
│   ├── connector_brz_category.json     ← Single table example
│   └── connector_brz_all_tables.json   ← All tables config
└── sink/
    └── connector_azure_ecommerce_all.json ← Azure sink config

postgres/
└── init.sql                            ← Updated with bronze tables

docker-compose.yml                      ← Updated with event-generator service

QUICKSTART_EVENT_GENERATOR.md           ← Quick start guide
EVENT_GENERATOR_INTEGRATION.md          ← This file
```

## Data Model

### Dimension Tables

#### brz_category
```sql
category_code   VARCHAR(50) PK    -- e.g., "CAT-ELEC"
category_name   VARCHAR(255)      -- e.g., "Electronics"
dt_update       BIGINT            -- Timestamp in milliseconds
```

#### brz_brands
```sql
brand_code      VARCHAR(50) PK    -- e.g., "BRD-NOVACORP"
brand_name      VARCHAR(255)      -- e.g., "NovaCorp"
category_code   VARCHAR(50) FK    -- Links to brz_category
dt_update       BIGINT
```

#### brz_customers
```sql
customer_id     VARCHAR(50) PK    -- e.g., "cust_000001"
phone           VARCHAR(50)       -- e.g., "+1987654321"
country_code    VARCHAR(10)       -- e.g., "US"
country         VARCHAR(100)      -- e.g., "United States"
state           VARCHAR(100)      -- e.g., "California"
city            VARCHAR(100)      -- e.g., "Los Angeles"
dt_update       BIGINT
```

#### brz_calendar
```sql
date_code       INTEGER PK        -- e.g., 20240115 (YYYYMMDD)
full_date       DATE              -- e.g., 2024-01-15
year            INTEGER           -- e.g., 2024
quarter         INTEGER           -- 1-4
month           INTEGER           -- 1-12
month_name      VARCHAR(20)       -- e.g., "January"
day             INTEGER           -- 1-31
day_of_week     INTEGER           -- 1=Mon, 7=Sun
day_name        VARCHAR(20)       -- e.g., "Monday"
is_weekend      BOOLEAN           -- true if Sat/Sun
dt_update       BIGINT
```

#### brz_products
```sql
product_id      VARCHAR(50) PK    -- e.g., "prod_00001"
product_name    VARCHAR(255)      -- e.g., "Wireless Headphones"
brand_code      VARCHAR(50) FK    -- Links to brz_brands
category_code   VARCHAR(50) FK    -- Links to brz_category
price           DECIMAL(10,2)     -- e.g., 149.99
description     TEXT              -- Product description
dt_update       BIGINT
```

### Fact Table

#### brz_order_items
```sql
order_item_id   VARCHAR(50) PK    -- e.g., "oi_0000001"
order_id        VARCHAR(50)       -- e.g., "order_0001234"
customer_id     VARCHAR(50) FK    -- Links to brz_customers
product_id      VARCHAR(50) FK    -- Links to brz_products
date_code       INTEGER FK        -- Links to brz_calendar
quantity        INTEGER           -- e.g., 2
unit_price      DECIMAL(10,2)     -- Price per item
total_price     DECIMAL(10,2)     -- quantity * unit_price
dt_update       BIGINT
```

## Data Characteristics

### Variety & Realism

- **130 fictional brands** - Combinatorial generation from prefixes/suffixes
- **35+ cities across 20+ countries** - Realistic geographic distribution
- **160+ unique product descriptions** - Grammatically correct, category-appropriate
- **8 product categories** - Electronics, Sports, Home, Clothing, Books, Beauty, Toys, Grocery
- **Realistic pricing** - Category-appropriate price ranges with realistic endings (.99, .95, .49)

### Volume (Per Generation Cycle)

| Table | Rows | Update Type |
|-------|------|-------------|
| brz_category | 8 | Full refresh |
| brz_brands | 130 | Full refresh |
| brz_customers | 300 | Full refresh |
| brz_calendar | 1,096 | Full refresh |
| brz_products | 160 | Full refresh |
| brz_order_items | 3,000 | Full refresh |

**Total**: ~4,694 rows per cycle (every 60 seconds by default)

## Integration Points

### 1. PostgreSQL → Kafka (JDBC Source Connector)

**Configuration**: `connectors/source/connector_brz_all_tables.json`

```json
{
  "name": "postgres-source-ecommerce-all",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "mode": "timestamp",
    "timestamp.column.name": "dt_update",
    "table.whitelist": "public.brz_category,public.brz_brands,...",
    "poll.interval.ms": "5000"
  }
}
```

**Behavior**:
- Polls every 5 seconds
- Detects new/updated rows via `dt_update` timestamp
- Creates one topic per table with prefix `ecommerce.`

### 2. Kafka → Azure (Blob Sink Connector)

**Configuration**: `connectors/sink/connector_azure_ecommerce_all.json`

```json
{
  "name": "azure-blob-sink-ecommerce-all",
  "config": {
    "connector.class": "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector",
    "topics": "ecommerce.brz_category,ecommerce.brz_brands,...",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "rotate.schedule.interval.ms": "300000"
  }
}
```

**Behavior**:
- Consumes from all `ecommerce.*` topics
- Partitions by hour: `year=2024/month=01/day=15/hour=14/`
- Rotates files every 5 minutes
- Stores in JSON format

### 3. Direct Azure Backup

The Event Generator also writes directly to Azure (optional):

```
landing-zone/
├── category/category_20240115_143000.json
├── brands/brands_20240115_143000.json
├── customers/customers_20240115_143000.json
├── calendar/calendar_20240115_143000.json
├── products/products_20240115_143000.json
└── order_items/order_items_20240115_143000.json
```

This provides:
- Immediate backup before Kafka processing
- Raw data for audit/recovery
- Independent of Kafka infrastructure

## Configuration

### Environment Variables

```bash
# PostgreSQL (Required)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password

# Azure (Optional)
AZURE_STORAGE_ACCOUNT_NAME=your_account
AZURE_STORAGE_ACCOUNT_KEY=your_key
AZURE_STORAGE_CONTAINER_NAME=xp-project-ecommece

# Generator Settings
EVENT_GENERATION_INTERVAL=60  # Seconds between cycles
```

### Customization

**Change generation frequency**:
```bash
EVENT_GENERATION_INTERVAL=30  # Every 30 seconds
```

**Adjust data volume** (edit `event_generator_postgres_azure.py`):
```python
brands_rows = generate_brands_table(num_brands=200)        # More brands
customers_rows = generate_customers_table(num_customers=500)  # More customers
order_items_rows = generate_order_items_table(..., num_order_items=5000)  # More orders
```

## Deployment

### Docker Compose

The Event Generator runs as a standard service:

```yaml
event-generator:
  build: ./scripts
  container_name: event-generator
  depends_on:
    postgres-ecommerce:
      condition: service_healthy
  environment:
    POSTGRES_HOST: postgres
    POSTGRES_PORT: 5432
    # ... other env vars
  restart: unless-stopped
```

### Startup Sequence

1. **PostgreSQL** starts with healthcheck
2. **Event Generator** waits for PostgreSQL to be healthy
3. **Event Generator** creates tables if they don't exist
4. **Event Generator** begins generating data every 60s
5. **Kafka Connect** (started in parallel) polls PostgreSQL
6. **Kafka Topics** receive data from PostgreSQL
7. **Azure Sink** writes Kafka data to Azure Blob Storage

### Health Monitoring

```bash
# Check if running
docker-compose ps event-generator

# View logs
docker logs -f event-generator

# Expected log pattern (every 60s):
# INFO - Generating synthetic e-commerce data...
# INFO - Wrote 8 rows to brz_category
# INFO - Wrote 130 rows to brz_brands
# INFO - Wrote 300 rows to brz_customers
# INFO - Wrote 1096 rows to brz_calendar
# INFO - Wrote 160 rows to brz_products
# INFO - Wrote 3000 rows to brz_order_items
# INFO - Sleeping for 60 seconds...
```

## Testing & Validation

### Automated Verification

```bash
# Run comprehensive health check
bash scripts/verify_setup.sh
```

This checks:
- ✓ All Docker containers running
- ✓ PostgreSQL tables populated
- ✓ Kafka topics created
- ✓ Connectors in RUNNING state
- ✓ Event Generator active

### Manual Verification

**1. Check PostgreSQL**:
```bash
docker exec postgres psql -U postgres -d postgres -c "
  SELECT 'brz_category', COUNT(*) FROM brz_category
  UNION ALL
  SELECT 'brz_order_items', COUNT(*) FROM brz_order_items;
"
```

**2. Check Kafka Topics**:
```bash
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --list | grep ecommerce
```

**3. Consume Messages**:
```bash
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_order_items \
  --from-beginning \
  --max-messages 5
```

**4. Check Connectors**:
```bash
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status | jq
```

## Performance Considerations

### Database Load

- **Writes**: ~4,694 rows per minute (default interval)
- **Index overhead**: 13 indexes on bronze tables
- **Upsert strategy**: ON CONFLICT DO UPDATE (efficient)

**Expected PostgreSQL impact**: Low (suitable for dev/test)

### Kafka Throughput

- **Source connector**: Polls every 5 seconds
- **Topics**: 6 topics, ~4,694 messages/minute total
- **Message size**: Varies (100 bytes - 1 KB)

**Expected Kafka impact**: Minimal

### Azure Blob Storage

- **Direct writes**: 6 files per generation cycle
- **Kafka writes**: Batched, rotated every 5 minutes
- **Storage growth**: ~2-5 MB per day (JSON format)

## Troubleshooting

### Common Issues

**Event Generator fails to start**:
```bash
# Check PostgreSQL is ready
docker-compose ps postgres

# View detailed logs
docker logs event-generator

# Solution: Ensure PostgreSQL healthcheck passes
docker-compose up -d postgres
sleep 10
docker-compose up -d event-generator
```

**No data in PostgreSQL**:
```bash
# Check generator logs
docker logs event-generator | grep ERROR

# Check if tables exist
docker exec postgres psql -U postgres -d postgres -c "\dt"

# Solution: Restart generator
docker-compose restart event-generator
```

**No Kafka topics**:
```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-source-ecommerce-all/status

# Solution: Set up connectors
bash scripts/setup_connectors.sh
```

**Azure writes failing**:
```bash
# Check logs for Azure errors
docker logs event-generator | grep -i azure

# Common causes:
# - Missing credentials → Set AZURE_STORAGE_ACCOUNT_KEY
# - Invalid container → Create container in Azure Portal
# - Network issues → Check Azure connectivity

# Generator continues working (PostgreSQL unaffected)
```

## Migration & Compatibility

### Existing Systems

The Event Generator is **fully compatible** with existing infrastructure:

- ✅ Uses standard PostgreSQL tables
- ✅ Standard Kafka Connect connectors
- ✅ No changes to existing Tesouro Direto tables
- ✅ Can run alongside other data generators
- ✅ Can be disabled without affecting other services

### Disable Event Generator

```bash
# Stop the generator
docker-compose stop event-generator

# Or remove from docker-compose.yml
# Remove the event-generator service definition
```

## Future Enhancements

Potential improvements:

1. **Variable data patterns** - Seasonal trends, growth patterns
2. **Real-time streaming** - Sub-second data generation
3. **Multiple scenarios** - Black Friday, seasonal sales, etc.
4. **Data quality issues** - Intentional anomalies for testing
5. **Schema evolution** - Simulate schema changes
6. **Multi-region** - Generate data for specific geographies

## Summary

The Event Generator integration provides:

✅ **Automated data generation** - No manual data entry needed  
✅ **Realistic test data** - Variety in brands, locations, products  
✅ **Dual storage** - PostgreSQL + Azure backup  
✅ **Kafka integration** - Automatic streaming via JDBC connector  
✅ **Configurable** - Adjustable volume, frequency, data characteristics  
✅ **Production-ready** - Docker containerized, health checks, logging  
✅ **Well-documented** - Multiple guides and automated scripts  

This creates a complete, automated e-commerce analytics pipeline suitable for development, testing, and demonstration purposes.
