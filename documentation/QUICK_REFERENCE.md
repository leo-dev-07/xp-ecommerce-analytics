# Quick Reference Card

## 🚀 One-Line Commands

```bash
# Start everything
docker-compose up -d

# Stop everything
docker-compose down

# Restart event generator
docker-compose restart event-generator

# View logs
docker logs -f event-generator

# Check status
bash scripts/verify_setup.sh

# Setup connectors
bash scripts/setup_connectors.sh
```

## 📊 Check Data

```bash
# PostgreSQL row counts
docker exec postgres psql -U postgres -d postgres -c "
  SELECT 'brz_order_items', COUNT(*) FROM brz_order_items;
"

# Kafka topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.brz_order_items \
  --from-beginning --max-messages 5

# Connector status
curl http://localhost:8083/connectors/azure-blob-sink-ecommerce-all/status | jq
```

## 📁 Azure Blob Structure

```
Container: xp-project-ecommece
├── landing-zone/{table}/{table}_{timestamp}.json        (Direct backup)
└── kafka-data/{topic}/year=YYYY/month=MM/day=DD/hour=HH/ (Streaming)
```

## 🔧 Configuration

```bash
# .env file
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=postgres
AZURE_STORAGE_ACCOUNT_NAME=your_account
AZURE_STORAGE_ACCOUNT_KEY=your_key
AZURE_STORAGE_CONTAINER_NAME=xp-project-ecommece
EVENT_GENERATION_INTERVAL=60
```

## 📊 Tables

| Table | Rows | Key |
|-------|------|-----|
| brz_category | 8 | category_code |
| brz_brands | 130+ | brand_code |
| brz_customers | 300 | customer_id |
| brz_calendar | 1,096 | date_code |
| brz_products | 160 | product_id |
| brz_order_items | 3,000 | order_item_id |

## 🔄 Data Flow

```
Event Generator (60s) 
  → PostgreSQL (4,694 rows) 
  → Kafka Topics (6 topics) 
  → Azure Blob (time-partitioned)
```

## 📖 Documentation

- **Quick Start**: GET_STARTED.md
- **Full Guide**: QUICKSTART_EVENT_GENERATOR.md
- **Azure Structure**: AZURE_BLOB_STRUCTURE.md
- **Testing**: TESTING_RESULTS.md
- **Final Summary**: FINAL_SUMMARY.md

## ✅ Status Check

```bash
docker-compose ps
# All should show "Up"

curl http://localhost:8083/connectors | jq
# Should show 3 connectors

docker logs event-generator --tail 20
# Should show "Wrote X rows" messages
```

## 🎯 Success Indicators

✅ Event generator running  
✅ PostgreSQL has data  
✅ 6 Kafka topics exist  
✅ Connectors show RUNNING  
✅ Azure files have timestamps  
✅ Time-based folders created  

---

**Need help?** Run `bash scripts/verify_setup.sh`
