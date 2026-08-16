# Azure Blob Storage Structure

## Overview

The Event Generator and Kafka Connect write data to Azure Blob Storage in two different structures:

1. **Direct Backup** (from Event Generator)
2. **Kafka Streaming Data** (from Azure Blob Sink Connector)

## 1. Direct Backup Structure (Event Generator)

### Path Format
```
landing-zone/{table_name}/{table_name}_{timestamp}.json
```

### Example Files
```
landing-zone/
├── category/
│   └── category_20260816_071141.json
├── brands/
│   └── brands_20260816_071141.json
├── customers/
│   └── customers_20260816_071141.json
├── calendar/
│   └── calendar_20260816_071141.json
├── products/
│   └── products_20260816_071141.json
└── order_items/
    └── order_items_20260816_071141.json
```

### File Content Example
```json
[
  {
    "category_code": "CAT-ELEC",
    "category_name": "Electronics",
    "dt_update": 1786864301491
  },
  {
    "category_code": "CAT-SPRT",
    "category_name": "Sports & Outdoors",
    "dt_update": 1786864301491
  }
]
```

### Characteristics
- **Format**: Complete array of all records per table
- **Frequency**: Generated every 60 seconds (configurable)
- **Size**: Full dataset per file (e.g., 3000 order items per file)
- **Use case**: Bulk backup, historical snapshots

## 2. Kafka Streaming Data (Connector)

### Path Format
```
kafka-data/{topic_name}/year={YYYY}/month={MM}/day={DD}/hour={HH}/{topic_name}+{partition}+{offset}.json
```

### Example Files
```
kafka-data/
├── ecommerce.brz_category/
│   └── year=2026/
│       └── month=08/
│           └── day=16/
│               └── hour=07/
│                   ├── ecommerce.brz_category+0+0000000000.json
│                   ├── ecommerce.brz_category+0+0000000100.json
│                   └── ecommerce.brz_category+0+0000000200.json
├── ecommerce.brz_brands/
│   └── year=2026/
│       └── month=08/
│           └── day=16/
│               └── hour=07/
│                   ├── ecommerce.brz_brands+0+0000000000.json
│                   └── ecommerce.brz_brands+0+0000000100.json
├── ecommerce.brz_customers/
│   └── year=2026/...
├── ecommerce.brz_calendar/
│   └── year=2026/
│       └── month=08/
│           └── day=16/
│               └── hour=07/
│                   ├── ecommerce.brz_calendar+0+0000002192.json
│                   └── ecommerce.brz_calendar+0+0000002292.json
├── ecommerce.brz_products/
│   └── year=2026/...
└── ecommerce.brz_order_items/
    └── year=2026/...
```

### File Content Example
```json
{"category_code":"CAT-ELEC","category_name":"Electronics","dt_update":1786864425683}
{"category_code":"CAT-SPRT","category_name":"Sports & Outdoors","dt_update":1786864425683}
{"category_code":"CAT-HOME","category_name":"Home & Kitchen","dt_update":1786864425683}
```

### Characteristics
- **Format**: Newline-delimited JSON (one record per line)
- **Frequency**: Every 60 seconds or 100 records (whichever comes first)
- **Size**: Smaller incremental files (100 records each)
- **Partitioning**: Time-based (hourly folders)
- **Use case**: Incremental streaming, analytics queries

## Configuration Details

### Event Generator Configuration
```python
# Direct to Azure - timestamp in filename
blob_name = f"landing-zone/{table_name}/{table_name}_{datetime_utc}.json"
```

### Kafka Connector Configuration
```json
{
  "topics.dir": "kafka-data",
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
  "partition.duration.ms": "3600000",
  "timestamp.extractor": "RecordField",
  "timestamp.field": "dt_update",
  "flush.size": "100",
  "rotate.interval.ms": "60000"
}
```

### Key Settings
| Setting | Value | Purpose |
|---------|-------|---------|
| `partitioner.class` | `TimeBasedPartitioner` | Enables time-based folders |
| `path.format` | `'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH` | Hive-style partitioning |
| `partition.duration.ms` | `3600000` | 1 hour partitions |
| `timestamp.extractor` | `RecordField` | Extract timestamp from record |
| `timestamp.field` | `dt_update` | Use dt_update field for partitioning |
| `flush.size` | `100` | Flush after 100 records |
| `rotate.interval.ms` | `60000` | Or flush every 60 seconds |

## File Naming Convention

### Kafka Connector Files
```
{topic_name}+{partition}+{offset}.json
```

Examples:
- `ecommerce.brz_category+0+0000000000.json` - First file, partition 0, offset 0
- `ecommerce.brz_calendar+0+0000002192.json` - Partition 0, offset 2192
- `ecommerce.brz_order_items+0+0000012345.json` - Partition 0, offset 12345

### Event Generator Files
```
{table_name}_{YYYYMMdd_HHmmss}.json
```

Examples:
- `category_20260816_071141.json` - Generated at 2026-08-16 07:11:41
- `order_items_20260816_073000.json` - Generated at 2026-08-16 07:30:00

## Timestamp in Data

All records include a `dt_update` field with milliseconds since epoch:

```json
{
  "category_code": "CAT-ELEC",
  "category_name": "Electronics",
  "dt_update": 1786864301491  // ← Timestamp (Aug 16, 2026 07:11:41 UTC)
}
```

To convert to human-readable:
```python
import datetime
timestamp_ms = 1786864301491
dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)
print(dt)  # 2026-08-16 07:11:41.491000
```

## Querying Data

### By Time (Hive-style partitioning)
```sql
-- Query specific hour
SELECT * FROM kafka_data
WHERE year = 2026 
  AND month = 8 
  AND day = 16 
  AND hour = 7;

-- Query specific day
SELECT * FROM kafka_data
WHERE year = 2026 
  AND month = 8 
  AND day = 16;
```

### By Timestamp Field
```sql
-- Query using dt_update
SELECT * FROM kafka_data
WHERE dt_update >= 1786864301491 
  AND dt_update < 1786867901491;
```

## Benefits of This Structure

### Direct Backup (landing-zone/)
✅ Full snapshots at each generation  
✅ Easy to restore entire dataset  
✅ Simple file structure  
✅ Good for historical analysis  

### Kafka Streaming (kafka-data/)
✅ Incremental updates  
✅ Efficient for large datasets  
✅ Time-based partitioning for analytics  
✅ Hive/Spark compatible structure  
✅ Easy to query specific time ranges  

## Storage Optimization

### Retention Policies (Recommended)
```
landing-zone/: Keep last 7 days (full snapshots)
kafka-data/: Keep last 90 days (incremental data)
```

### File Sizes
- **Direct backup**: 1-10 MB per table per cycle
- **Kafka streaming**: 10-100 KB per file (100 records)

### Daily Growth Estimate
```
Direct Backup:  6 tables × 24 hours × 5 MB = 720 MB/day
Kafka Streaming: 6 topics × 1440 files × 50 KB = 432 MB/day
Total: ~1.2 GB/day
```

## Access Patterns

### For Analytics (Recommended)
Use **kafka-data/** with time partitioning:
- Query specific hours/days efficiently
- Compatible with Azure Data Lake Analytics, Databricks, Spark
- Incremental processing

### For Backup/Recovery
Use **landing-zone/**:
- Complete snapshots
- Easy restoration
- Audit trail

## Example Azure Storage Explorer View

```
Container: xp-project-ecommece
│
├── landing-zone/
│   ├── category/
│   │   ├── category_20260816_070000.json (5 KB)
│   │   ├── category_20260816_071000.json (5 KB)
│   │   └── category_20260816_072000.json (5 KB)
│   └── order_items/
│       ├── order_items_20260816_070000.json (2.1 MB)
│       └── order_items_20260816_071000.json (2.1 MB)
│
└── kafka-data/
    ├── ecommerce.brz_category/
    │   └── year=2026/month=08/day=16/hour=07/
    │       ├── ecommerce.brz_category+0+0000000000.json (2 KB)
    │       └── ecommerce.brz_category+0+0000000008.json (2 KB)
    └── ecommerce.brz_order_items/
        └── year=2026/month=08/day=16/hour=07/
            ├── ecommerce.brz_order_items+0+0000000000.json (15 KB)
            ├── ecommerce.brz_order_items+0+0000000100.json (15 KB)
            └── ecommerce.brz_order_items+0+0000000200.json (15 KB)
```

## Troubleshooting

### Files without timestamps
**Problem**: Files named like `ecommerce_events+0+0000000000.json`

**Cause**: Using `DefaultPartitioner` instead of `TimeBasedPartitioner`

**Solution**: Update connector config with:
```json
{
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "timestamp.extractor": "RecordField",
  "timestamp.field": "dt_update"
}
```

### No time-based folders
**Problem**: All files in root of topic folder

**Cause**: Missing `path.format` configuration

**Solution**: Add:
```json
{
  "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
}
```

### Wrong partition duration
**Problem**: Too many/few time partitions

**Solution**: Adjust `partition.duration.ms`:
```json
{
  "partition.duration.ms": "3600000"  // 1 hour
  "partition.duration.ms": "86400000" // 1 day
  "partition.duration.ms": "900000"   // 15 minutes
}
```

---

**Updated**: December 16, 2024  
**Status**: ✅ Fully Configured and Tested  
**Connector Version**: Azure Blob Storage Sink 2.2.7
