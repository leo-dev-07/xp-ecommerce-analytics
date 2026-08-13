from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BinaryType, NullType, FloatType
from pyspark.sql import functions as F

catalog_name = 'ecommerce'

### Brands

# Define schema for the data file
brand_schema = StructType([
    StructField('brand_code', StringType(), False),
    StructField('brand_name', StringType(), True),
    StructField('category_code', StringType(), True)
])

raw_data_path = f'/Volumes/ecommerce/source_data/raw/ecomm-raw-data/brands/*.csv'

brand_df = spark.read.format('csv').schema(brand_schema).option('header', 'true').option('delimiter', ',').load(raw_data_path)

brand_df = brand_df.withColumn('_source_file', F.col('_metadata.file_path'))\
                    .withColumn('ingested_at', F.current_timestamp())

# Write raw data to the Bronze layer
brand_df.write.format('delta')\
    .mode('overwrite')\
    .option('mergeSchema','true')\
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")

### Category

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/ecomm-raw-data/category/"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))

# Write raw data to the Bronze layer
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_category")     

### Products

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  # datatype is string due to incoming data contain anomalies
    StructField("length_cm", StringType(), True),     # datatype is string due to incoming data contain anomalies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/ecomm-raw-data/products/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path) \
    .withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")    

### Customers

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/ecomm-raw-data/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").csv(raw_data_path) \
    .withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("_ingested_at", F.current_timestamp())

# Write raw data to the Bronze layer
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")

### Date

# Define schema for the data file
date_schema = StructType([
    StructField("date", StringType(), True),           # Raw date in string format
    StructField("year", IntegerType(), True),          # Year
    StructField("day_name", StringType(), True),       # Day name (can be mixed case)
    StructField("quarter", IntegerType(), True),       # Quarter
    StructField("week_of_year", IntegerType(), True),  # Week of year (can be negative)
])

# Load data using the schema defined
raw_data_path = f"/Volumes/ecommerce/source_data/raw/ecomm-raw-data/date/*.csv" 

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))

# Write raw data to the Bronze layer
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_calendar")               

### Order Items

raw_data_path = "/Volumes/ecommerce/source_data/raw/ecomm-raw-data/order_items/landing/*csv" 

order_items_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("order_ts", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("item_seq", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True), 
    StructField("discount_pct", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("coupon_code", StringType(), True)  # Fixed typo: was 'copoun_code'
])

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(order_items_schema).csv(raw_data_path)
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))

# Write raw data to the Bronze layer
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_order_items")