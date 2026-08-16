# Get Started - E-commerce Event Generator

**Quick setup guide to get your automated e-commerce data pipeline running in 5 minutes!**

## What You're Building

```
Event Generator → PostgreSQL → Kafka → Azure Blob Storage
     ↓
  Every 60s: 4,700 rows of synthetic e-commerce data
     ↓
  6 tables: categories, brands, customers, calendar, products, orders
```

## Prerequisites

- ✅ Docker & Docker Compose installed
- ✅ (Optional) Azure Blob Storage account

## Step 1: Environment Setup (2 minutes)

Create a `.env` file in the project root:

```bash
cat > .env << 'EOF'
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=postgres

# Azure Blob Storage (Optional - leave empty if you don't have it)
AZURE_STORAGE_ACCOUNT_NAME=
AZURE_STORAGE_ACCOUNT_KEY=
AZURE_STORAGE_CONTAINER_NAME=xp-project-ecommece

# Event Generator
EVENT_GENERATION_INTERVAL=60
EOF
```

**Don't have Azure?** No problem! Just leave those fields empty. The system will work perfectly with PostgreSQL and Kafka only.

## Step 2: Start Everything (1 minute)

```bash
# Build and start all containers
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
sleep 30

# Check all services are running
docker-compose ps
```

You should see these 6 containers running:
- ✅ zookeeper
- ✅ broker (Kafka)
- ✅ schema-registry
- ✅ connect (Kafka Connect)
- ✅ postgres
- ✅ **event-generator** (NEW!)

## Step 3: Verify Data Generation (1 minute)

```bash
# Check Event Generator logs (should show "Wrote X rows..." messages)
docker logs --tail 20 event-generator

# Connect to PostgreSQL and check data
docker exec -it postgres psql -U postgres -d postgres

# Inside psql, run:
\dt                    -- List tables (you should see brz_* tables)

SELECT COUNT(*) FROM brz_order_items;  -- Should show 3000 rows

\q                     -- Exit
```

## Step 4: Set Up Kafka Connectors (1 minute)

```bash
# Make scripts executable
chmod +x scripts/setup_connectors.sh scripts/verify_setup.sh

# Run the connector setup script
bash scripts/setup_connectors.sh
```

This creates:
1. **PostgreSQL → Kafka** source connector
2. **Kafka → Azure** sink connector (if Azure credentials are set)

## Step 5: Verify Everything Works (1 minute)

```bash
# Run the comprehensive verification script
bash scripts/verify_setup.sh
```

This checks:
- ✓ All services running
- ✓ Data in PostgreSQL
- ✓ Kafka topics created
- ✓ Connectors active
- ✓ Event generator working

## You're Done! 🎉

Your automated e-commerce data pipeline is now running!

### What's Happening Now?

Every 60 seconds, the Event Generator:
1. Creates 4,700 rows of synthetic data
2. Writes to 6 PostgreSQL tables
3. Kafka Connect reads the new data
4. Publishes to Kafka topics
5. (Optional) Writes to Azure Blob Storage

### View Live Data

**PostgreSQL:**
```bash
docker exec -it postgres psql -U postgres -d postgres
SELECT * FROM brz_products LIMIT 5;
SELECT * FROM brz_order_items ORDER BY dt_update DESC LIMIT 10;
```

**Kafka Topics:**
```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_order_items \
  --from-beginning --max-messages 5
```

**Event Generator Logs:**
```bash
docker logs -f event-generator
```

## Common Commands

```bash
# View all logs
docker-compose logs -f

# Restart a service
docker-compose restart event-generator

# Stop everything
docker-compose down

# Start everything
docker-compose up -d

# Check connector status
curl http://localhost:8083/connectors | jq
```

## Troubleshooting

### Event Generator keeps restarting?

```bash
# Check logs
docker logs event-generator

# Usually means PostgreSQL isn't ready yet
docker-compose restart postgres
sleep 10
docker-compose restart event-generator
```

### No Kafka topics?

```bash
# Set up connectors
bash scripts/setup_connectors.sh
```

### Need more help?

Run the verification script for diagnostics:
```bash
bash scripts/verify_setup.sh
```

## Next Steps

### Customize Data Volume

Edit `.env`:
```bash
EVENT_GENERATION_INTERVAL=30  # Generate every 30 seconds
```

### Explore the Data

```sql
-- Top selling products
SELECT 
    p.product_name,
    SUM(oi.quantity) as total_sold,
    SUM(oi.total_price) as revenue
FROM brz_order_items oi
JOIN brz_products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 10;

-- Sales by country
SELECT 
    c.country,
    COUNT(DISTINCT oi.order_id) as orders,
    SUM(oi.total_price) as revenue
FROM brz_order_items oi
JOIN brz_customers c ON oi.customer_id = c.customer_id
GROUP BY c.country
ORDER BY revenue DESC;
```

### Build Analytics

The data is now ready for:
- ✅ Real-time streaming analytics
- ✅ Data warehouse loading
- ✅ Machine learning pipelines
- ✅ Dashboard and reporting
- ✅ Testing and development

## Documentation

- **Quick Start** (you are here): `GET_STARTED.md`
- **Detailed Guide**: `scripts/README_EVENT_GENERATOR.md`
- **Full Quick Start**: `QUICKSTART_EVENT_GENERATOR.md`
- **Integration Guide**: `EVENT_GENERATOR_INTEGRATION.md`
- **Implementation Summary**: `IMPLEMENTATION_SUMMARY.md`

## Architecture

```
┌─────────────────────┐
│  Event Generator    │  Python container, runs every 60s
└──────────┬──────────┘
           │
           ├──────────────────────────┐
           ▼                          ▼
   ┌───────────────┐        ┌─────────────────┐
   │  PostgreSQL   │        │ Azure Blob (opt)│
   │  6 tables     │        │ Direct backup   │
   │  4,700 rows   │        └─────────────────┘
   └───────┬───────┘
           │ JDBC Source Connector (polls every 5s)
           ▼
   ┌───────────────┐
   │ Kafka Topics  │  ecommerce.brz_category
   │ (6 topics)    │  ecommerce.brz_brands
   │               │  ecommerce.brz_customers
   │               │  ecommerce.brz_calendar
   │               │  ecommerce.brz_products
   │               │  ecommerce.brz_order_items
   └───────┬───────┘
           │ Azure Sink Connector
           ▼
   ┌───────────────┐
   │ Azure Blob    │  Time-partitioned
   │ Storage       │  year/month/day/hour
   └───────────────┘
```

## Data Tables

| Table | Rows | Description |
|-------|------|-------------|
| brz_category | 8 | Electronics, Sports, Home, Clothing, Books, Beauty, Toys, Grocery |
| brz_brands | 130 | Fictional brand names across all categories |
| brz_customers | 300 | Customers in 35+ cities, 20+ countries |
| brz_calendar | 1,096 | Date dimension for 2024-2026 |
| brz_products | 160 | Products with realistic descriptions and pricing |
| brz_order_items | 3,000 | Order transactions linking customers, products, dates |

**Total**: 4,694 rows generated every cycle

## Key Features

- ✅ **Fully automated** - No manual data entry needed
- ✅ **Realistic data** - 130 brands, 35+ cities, varied products
- ✅ **Continuous generation** - Every 60 seconds (configurable)
- ✅ **Dual storage** - PostgreSQL + Azure backup
- ✅ **Kafka integration** - Automatic streaming via connectors
- ✅ **Production-ready** - Docker containerized with health checks
- ✅ **Well-documented** - 5 comprehensive guides
- ✅ **Easy to customize** - Simple configuration and code

## Support & Help

1. **Quick diagnostics**: `bash scripts/verify_setup.sh`
2. **Check logs**: `docker-compose logs`
3. **Read docs**: Start with `QUICKSTART_EVENT_GENERATOR.md`
4. **Database access**: `docker exec -it postgres psql -U postgres -d postgres`

---

**Ready to go?** Start with Step 1 above! ⬆️

**Questions?** Check `QUICKSTART_EVENT_GENERATOR.md` for more detailed instructions.

**Issues?** Run `bash scripts/verify_setup.sh` for automated diagnostics.

---

Enjoy your automated e-commerce data pipeline! 🚀
