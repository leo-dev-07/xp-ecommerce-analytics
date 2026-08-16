#!/usr/bin/env python3
"""
E-commerce Event Generator for PostgreSQL and Azure Blob Storage

This script generates synthetic e-commerce data and writes it to both:
1. PostgreSQL database (which feeds Kafka via JDBC connector)
2. Azure Blob Storage (for backup/analytics)

The script runs continuously, generating batches of events every minute.

Tables generated (ecommerce.bronze schema):
- brz_category
- brz_brands  
- brz_customers
- brz_calendar
- brz_products
- brz_order_items

Environment Variables Required:
- POSTGRES_HOST (default: postgres)
- POSTGRES_PORT (default: 5432)
- POSTGRES_DB (default: postgres)
- POSTGRES_USER (default: postgres)
- POSTGRES_PASSWORD (default: password)
- AZURE_STORAGE_ACCOUNT_NAME (optional)
- AZURE_STORAGE_ACCOUNT_KEY (optional)
- AZURE_STORAGE_CONTAINER_NAME (default: xp-project-ecommece)
- EVENT_GENERATION_INTERVAL (default: 60 seconds)
"""

import os
import sys
import json
import time
import random
import itertools
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import Azure Blob Writer (optional if running without Azure)
try:
    from azure_blob_writer import AzureBlobEventWriter
    AZURE_AVAILABLE = True
except ImportError:
    logger.warning("Azure Blob Writer not available. Will only write to PostgreSQL.")
    AZURE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration from Environment Variables
# ---------------------------------------------------------------------------
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password'),
}

AZURE_CONFIG = {
    'account_name': os.getenv('AZURE_STORAGE_ACCOUNT_NAME'),
    'account_key': os.getenv('AZURE_STORAGE_ACCOUNT_KEY'),
    'container_name': os.getenv('AZURE_STORAGE_CONTAINER_NAME', 'xp-project-ecommece'),
}

EVENT_GENERATION_INTERVAL = int(os.getenv('EVENT_GENERATION_INTERVAL', '60'))

# ---------------------------------------------------------------------------
# Schema Definitions (same as original script)
# ---------------------------------------------------------------------------
CATEGORY_DEFINITIONS = [
    {
        "category_code": "CAT-ELEC", "category_name": "Electronics",
        "price_range": (15.0, 1499.99),
        "nouns": ["Headphones", "Speaker", "Smartphone", "Laptop", "Tablet", "Smartwatch",
                   "Camera", "Router", "Monitor", "Keyboard", "Mouse", "Drone", "Power Bank",
                   "Earbuds", "Webcam", "External SSD", "Projector", "Smart Plug"],
        "benefits": ["crystal-clear audio", "all-day battery life", "seamless wireless connectivity",
                      "fast charging", "stunning visual clarity", "effortless multitasking",
                      "reliable long-range signal", "smart home integration", "studio-quality sound",
                      "on-the-go convenience", "ultra-fast data transfer"],
    },
    {
        "category_code": "CAT-SPRT", "category_name": "Sports & Outdoors",
        "price_range": (9.99, 349.99),
        "nouns": ["Running Shoes", "Yoga Mat", "Water Bottle", "Backpack", "Tent",
                   "Bicycle Helmet", "Resistance Bands", "Hiking Boots", "Fitness Tracker",
                   "Camping Chair", "Dumbbell Set", "Sleeping Bag", "Cycling Jersey",
                   "Trekking Poles", "Cooler", "Kayak Paddle", "Jump Rope"],
        "benefits": ["all-day comfort on the trail", "reliable grip and support", "lightweight portability",
                      "durability in rugged conditions", "breathable performance fabric",
                      "confidence on every workout", "weatherproof protection", "easy setup and storage"],
    },
    {
        "category_code": "CAT-HOME", "category_name": "Home & Kitchen",
        "price_range": (12.99, 599.99),
        "nouns": ["Coffee Maker", "Desk Lamp", "Blender", "Air Fryer", "Vacuum Cleaner",
                   "Cookware Set", "Toaster", "Bedding Set", "Throw Pillow", "Wall Clock",
                   "Storage Bin", "Cutting Board", "Humidifier", "Candle Set", "Curtain Panel",
                   "Knife Set", "Area Rug"],
        "benefits": ["effortless everyday cooking", "a cozy modern look for any room",
                      "energy-efficient performance", "quick and easy cleanup",
                      "a clutter-free living space", "restful sleep", "reliable everyday performance"],
    },
    {
        "category_code": "CAT-CLTH", "category_name": "Clothing & Apparel",
        "price_range": (9.99, 199.99),
        "nouns": ["T-Shirt", "Denim Jacket", "Hoodie", "Sneakers", "Sundress", "Wool Sweater",
                   "Cargo Pants", "Baseball Cap", "Rain Jacket", "Leather Belt", "Scarf",
                   "Sunglasses", "Pajama Set", "Swim Trunks", "Flannel Shirt", "Ankle Boots"],
        "benefits": ["all-day comfort", "a versatile everyday look", "breathable warm-weather wear",
                      "cozy cold-weather warmth", "a flattering modern fit", "durability wash after wash"],
    },
    {
        "category_code": "CAT-BOOK", "category_name": "Books & Media",
        "price_range": (6.99, 49.99),
        "nouns": ["Mystery Novel", "Cookbook", "Graphic Novel", "Poetry Collection", "Travel Guide",
                   "Biography", "Picture Book", "Self-Help Guide", "Fantasy Novel", "History Book",
                   "Puzzle Book", "Journal", "Board Game", "Card Game", "Coloring Book"],
        "benefits": ["hours of engaging reading", "a thoughtful gift for any occasion",
                      "a relaxing way to unwind", "inspiration for creative projects",
                      "fun for the whole family"],
    },
    {
        "category_code": "CAT-BUTY", "category_name": "Beauty & Personal Care",
        "price_range": (4.99, 149.99),
        "nouns": ["Facial Cleanser", "Moisturizer", "Shampoo", "Electric Toothbrush", "Hair Dryer",
                   "Makeup Palette", "Perfume", "Sunscreen", "Beard Trimmer", "Nail Kit",
                   "Body Lotion", "Lip Balm Set", "Skincare Set", "Hair Straightener", "Bath Bomb Set"],
        "benefits": ["a refreshed daily routine", "healthy glowing skin", "salon-quality results at home",
                      "long-lasting freshness", "gentle care for sensitive skin"],
    },
    {
        "category_code": "CAT-TOYS", "category_name": "Toys & Games",
        "price_range": (7.99, 129.99),
        "nouns": ["Building Block Set", "Remote Control Car", "Plush Toy", "Jigsaw Puzzle",
                   "Action Figure", "Board Game", "Art Supply Kit", "Toy Train Set", "Dollhouse",
                   "Science Kit", "Card Game", "Outdoor Play Tent", "Musical Toy", "Educational Tablet"],
        "benefits": ["hours of imaginative play", "hands-on learning fun", "screen-free entertainment",
                      "a memorable gift for kids", "creative skill-building"],
    },
    {
        "category_code": "CAT-GROC", "category_name": "Grocery & Gourmet",
        "price_range": (3.99, 39.99),
        "nouns": ["Coffee Beans", "Olive Oil", "Pasta", "Herbal Tea", "Granola", "Hot Sauce",
                   "Dark Chocolate Bar", "Spice Set", "Honey Jar", "Trail Mix", "Sparkling Water",
                   "Protein Powder", "Nut Butter", "Dried Fruit Mix", "Cooking Sauce"],
        "benefits": ["a flavorful addition to any meal", "a wholesome everyday snack",
                      "gourmet quality at home", "a healthier pantry staple", "bold, satisfying flavor"],
    },
]

ADJECTIVES = ["Wireless", "Portable", "Compact", "Premium", "Rugged", "Ultra-Light", "Smart",
              "Ergonomic", "Eco-Friendly", "Professional", "Everyday", "Deluxe", "Classic",
              "Modern", "Heavy-Duty", "Rechargeable", "Adjustable", "All-Weather",
              "Multi-Purpose", "Foldable", "Handcrafted", "Insulated", "Slim", "Vintage-Style"]

PLURAL_NOUNS = {
    "Headphones", "Earbuds", "Running Shoes", "Resistance Bands", "Hiking Boots",
    "Trekking Poles", "Sneakers", "Cargo Pants", "Sunglasses", "Swim Trunks", "Ankle Boots",
}

BRAND_PREFIXES = ["Nova", "Urban", "Prime", "Eco", "Swift", "Bright", "True", "Peak", "Vivid",
                   "Lumen", "Nimbus", "Solace", "Crest", "Orbit", "Vertex", "Harbor", "Meridian",
                   "Cobalt", "Ember", "Drift", "Alpine", "Cascade", "Fable", "Granite", "Halcyon",
                   "Ivory", "Juniper", "Kindle", "Lyric", "Mosaic", "Anchor", "Beacon", "Clover"]
BRAND_SUFFIXES = ["Corp", "Works", "Labs", "Co", "Hub", "Gear", "Craft", "Wave", "Line", "Group",
                   "Studio", "House", "Collective", "Supply", "Goods", "Forge", "Foundry",
                   "Society", "Union", "Traders"]

CUSTOMER_LOCATIONS = [
    ("New York", "New York", "United States", "US", "1"),
    ("Los Angeles", "California", "United States", "US", "1"),
    ("Chicago", "Illinois", "United States", "US", "1"),
    ("Houston", "Texas", "United States", "US", "1"),
    ("Miami", "Florida", "United States", "US", "1"),
    ("Seattle", "Washington", "United States", "US", "1"),
    ("Boston", "Massachusetts", "United States", "US", "1"),
    ("San Francisco", "California", "United States", "US", "1"),
    ("Denver", "Colorado", "United States", "US", "1"),
    ("Atlanta", "Georgia", "United States", "US", "1"),
    ("Toronto", "Ontario", "Canada", "CA", "1"),
    ("Vancouver", "British Columbia", "Canada", "CA", "1"),
    ("Montreal", "Quebec", "Canada", "CA", "1"),
    ("Mexico City", "Mexico City", "Mexico", "MX", "52"),
    ("Guadalajara", "Jalisco", "Mexico", "MX", "52"),
    ("Sao Paulo", "Sao Paulo", "Brazil", "BR", "55"),
    ("Rio de Janeiro", "Rio de Janeiro", "Brazil", "BR", "55"),
    ("Buenos Aires", "Buenos Aires", "Argentina", "AR", "54"),
    ("Santiago", "Santiago Metropolitan", "Chile", "CL", "56"),
    ("Lima", "Lima", "Peru", "PE", "51"),
    ("London", "England", "United Kingdom", "GB", "44"),
    ("Manchester", "England", "United Kingdom", "GB", "44"),
    ("Paris", "Ile-de-France", "France", "FR", "33"),
    ("Berlin", "Berlin", "Germany", "DE", "49"),
    ("Munich", "Bavaria", "Germany", "DE", "49"),
    ("Madrid", "Madrid", "Spain", "ES", "34"),
    ("Barcelona", "Catalonia", "Spain", "ES", "34"),
    ("Rome", "Lazio", "Italy", "IT", "39"),
    ("Milan", "Lombardy", "Italy", "IT", "39"),
    ("Amsterdam", "North Holland", "Netherlands", "NL", "31"),
    ("Tokyo", "Tokyo", "Japan", "JP", "81"),
    ("Seoul", "Seoul", "South Korea", "KR", "82"),
    ("Sydney", "New South Wales", "Australia", "AU", "61"),
    ("Mumbai", "Maharashtra", "India", "IN", "91"),
    ("Singapore", "Singapore", "Singapore", "SG", "65"),
]

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def make_description(adj: str, noun: str, benefit: str) -> str:
    """Compose a grammatically-correct description."""
    plural = noun in PLURAL_NOUNS
    noun_lower, adj_lower = noun.lower(), adj.lower()
    starts_vowel = adj[0].lower() in "aeiou"
    a_an_cap = "An" if starts_vowel else "A"
    a_an_low = "an" if starts_vowel else "a"

    if plural:
        templates = [
            f"{adj} {noun} built for {benefit}.",
            f"These {adj_lower} {noun_lower} deliver {benefit}.",
            f"{adj} {noun} designed with {benefit} in mind.",
            f"Enjoy {benefit} with {adj_lower} {noun_lower}.",
            f"{noun} engineered for {benefit}, with {a_an_low} {adj_lower} design.",
        ]
    else:
        templates = [
            f"{adj} {noun} built for {benefit}.",
            f"This {adj_lower} {noun_lower} delivers {benefit}.",
            f"{a_an_cap} {adj_lower} {noun_lower} designed with {benefit} in mind.",
            f"Enjoy {benefit} with {a_an_low} {adj_lower} {noun_lower}.",
            f"{noun} engineered for {benefit}, with {a_an_low} {adj_lower} design.",
        ]
    return random.choice(templates)


def make_price(min_price: float, max_price: float) -> float:
    """Random, realistic-looking price within a range."""
    raw = random.uniform(min_price, max_price)
    if raw < 20:
        return round(raw, 2)
    ending = random.choice([0.99, 0.95, 0.49, 0.00])
    return round(int(raw) + ending, 2)


# ---------------------------------------------------------------------------
# Data Generation Functions
# ---------------------------------------------------------------------------

def generate_category_table() -> List[Dict[str, Any]]:
    return [
        {"category_code": c["category_code"], "category_name": c["category_name"]}
        for c in CATEGORY_DEFINITIONS
    ]


def generate_brands_table(num_brands: int = 130) -> List[Dict[str, Any]]:
    """Generate num_brands unique fictional brand names."""
    combos = list(itertools.product(BRAND_PREFIXES, BRAND_SUFFIXES))
    random.shuffle(combos)
    num_brands = min(num_brands, len(combos))
    chosen = combos[:num_brands]

    brands = []
    for i, (prefix, suffix) in enumerate(chosen):
        category_code = CATEGORY_DEFINITIONS[i % len(CATEGORY_DEFINITIONS)]["category_code"]
        brands.append({
            "brand_code": f"BRD-{prefix.upper()}{suffix.upper()}",
            "brand_name": f"{prefix}{suffix}",
            "category_code": category_code,
        })
    return brands


def generate_customers_table(num_customers: int = 300) -> List[Dict[str, Any]]:
    customers = []
    for i in range(1, num_customers + 1):
        city, state, country, country_code, dial_code = random.choice(CUSTOMER_LOCATIONS)
        customers.append({
            "customer_id": f"cust_{i:06d}",
            "phone": f"+{dial_code}{random.randint(10**8, 10**9 - 1)}",
            "country_code": country_code,
            "country": country,
            "state": state,
            "city": city,
        })
    return customers


def generate_calendar_table(start: date, end: date) -> List[Dict[str, Any]]:
    rows = []
    current = start
    while current <= end:
        iso_weekday = current.isoweekday()
        rows.append({
            "date_code": int(current.strftime("%Y%m%d")),
            "full_date": current.isoformat(),
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "day": current.day,
            "day_of_week": iso_weekday,
            "day_name": current.strftime("%A"),
            "is_weekend": iso_weekday in (6, 7),
        })
        current += timedelta(days=1)
    return rows


def generate_products_table(brands: List[Dict[str, Any]], products_per_category: int = 20) -> List[Dict[str, Any]]:
    """Generate products for every category."""
    products = []
    idx = 1
    for cat in CATEGORY_DEFINITIONS:
        cat_code = cat["category_code"]
        min_p, max_p = cat["price_range"]
        cat_brands = [b for b in brands if b["category_code"] == cat_code] or brands
        used_descriptions = set()

        for _ in range(products_per_category):
            adj = random.choice(ADJECTIVES)
            noun = random.choice(cat["nouns"])
            benefit = random.choice(cat["benefits"])
            description = make_description(adj, noun, benefit)

            attempts = 0
            while description in used_descriptions and attempts < 6:
                adj = random.choice(ADJECTIVES)
                noun = random.choice(cat["nouns"])
                benefit = random.choice(cat["benefits"])
                description = make_description(adj, noun, benefit)
                attempts += 1
            used_descriptions.add(description)

            brand = random.choice(cat_brands)
            products.append({
                "product_id": f"prod_{idx:05d}",
                "product_name": f"{adj} {noun}",
                "brand_code": brand["brand_code"],
                "category_code": cat_code,
                "price": make_price(min_p, max_p),
                "description": description,
            })
            idx += 1
    return products


def generate_order_items_table(
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    calendar: List[Dict[str, Any]],
    num_order_items: int = 3000,
) -> List[Dict[str, Any]]:
    order_items = []
    order_id = None
    for i in range(1, num_order_items + 1):
        if order_id is None or random.random() < 0.4:
            order_id = f"order_{random.randint(1, num_order_items):07d}"

        customer = random.choice(customers)
        product = random.choice(products)
        cal_row = random.choice(calendar)
        quantity = random.randint(1, 5)
        unit_price = product["price"]

        order_items.append({
            "order_id": order_id,
            "order_item_id": f"oi_{i:07d}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "date_code": cal_row["date_code"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": round(unit_price * quantity, 2),
        })
    return order_items


# ---------------------------------------------------------------------------
# Database Functions
# ---------------------------------------------------------------------------

class PostgreSQLWriter:
    """Handles writing data to PostgreSQL database."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.config)
            logger.info(f"Connected to PostgreSQL at {self.config['host']}:{self.config['port']}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")
    
    def ensure_tables_exist(self):
        """Create tables if they don't exist."""
        create_statements = [
            """
            CREATE TABLE IF NOT EXISTS brz_category (
                category_code VARCHAR(50) PRIMARY KEY,
                category_name VARCHAR(255) NOT NULL,
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS brz_brands (
                brand_code VARCHAR(50) PRIMARY KEY,
                brand_name VARCHAR(255) NOT NULL,
                category_code VARCHAR(50),
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS brz_customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                phone VARCHAR(50),
                country_code VARCHAR(10),
                country VARCHAR(100),
                state VARCHAR(100),
                city VARCHAR(100),
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS brz_calendar (
                date_code INTEGER PRIMARY KEY,
                full_date DATE NOT NULL,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                month_name VARCHAR(20),
                day INTEGER,
                day_of_week INTEGER,
                day_name VARCHAR(20),
                is_weekend BOOLEAN,
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS brz_products (
                product_id VARCHAR(50) PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                brand_code VARCHAR(50),
                category_code VARCHAR(50),
                price DECIMAL(10, 2),
                description TEXT,
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS brz_order_items (
                order_item_id VARCHAR(50) PRIMARY KEY,
                order_id VARCHAR(50),
                customer_id VARCHAR(50),
                product_id VARCHAR(50),
                date_code INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10, 2),
                total_price DECIMAL(10, 2),
                dt_update BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
            )
            """
        ]
        
        try:
            with self.conn.cursor() as cur:
                for statement in create_statements:
                    cur.execute(statement)
                self.conn.commit()
            logger.info("All tables verified/created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            self.conn.rollback()
            raise
    
    def write_batch(self, table_name: str, rows: List[Dict[str, Any]]):
        """Write a batch of rows to a table."""
        if not rows:
            return
        
        # Add dt_update timestamp (milliseconds since epoch)
        dt_update = int(datetime.now().timestamp() * 1000)
        
        # Get column names from first row
        columns = list(rows[0].keys())
        columns.append('dt_update')
        
        # Prepare INSERT statement with ON CONFLICT DO UPDATE
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        
        # Create update clause for conflict resolution
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != list(rows[0].keys())[0]])
        
        insert_sql = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({list(rows[0].keys())[0]})
            DO UPDATE SET {update_clause}
        """
        
        # Prepare data tuples
        data = [tuple(list(row.values()) + [dt_update]) for row in rows]
        
        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, insert_sql, data)
                self.conn.commit()
            logger.info(f"Wrote {len(rows)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to write to {table_name}: {e}")
            self.conn.rollback()
            raise


# ---------------------------------------------------------------------------
# Main Event Generator
# ---------------------------------------------------------------------------

class EventGenerator:
    """Main event generator class."""
    
    def __init__(self):
        self.pg_writer = PostgreSQLWriter(POSTGRES_CONFIG)
        self.azure_writer = None
        
        if AZURE_AVAILABLE and AZURE_CONFIG['account_key']:
            try:
                self.azure_writer = AzureBlobEventWriter(
                    container_name=AZURE_CONFIG['container_name'],
                    credential=AZURE_CONFIG['account_key']
                )
                logger.info("Azure Blob Storage writer initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Azure writer: {e}")
        else:
            logger.info("Azure Blob Storage disabled (credentials not provided)")
    
    def initialize(self):
        """Initialize database connection and tables."""
        self.pg_writer.connect()
        self.pg_writer.ensure_tables_exist()
    
    def generate_and_write_data(self):
        """Generate synthetic data and write to PostgreSQL and Azure."""
        logger.info("Generating synthetic e-commerce data...")
        
        timestamp_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Generate all data
        category_rows = generate_category_table()
        brands_rows = generate_brands_table(num_brands=130)
        customers_rows = generate_customers_table(num_customers=300)
        calendar_rows = generate_calendar_table(date(2024, 1, 1), date(2026, 12, 31))
        products_rows = generate_products_table(brands_rows, products_per_category=20)
        order_items_rows = generate_order_items_table(
            customers_rows, products_rows, calendar_rows, num_order_items=3000
        )
        
        tables = {
            "brz_category": category_rows,
            "brz_brands": brands_rows,
            "brz_customers": customers_rows,
            "brz_calendar": calendar_rows,
            "brz_products": products_rows,
            "brz_order_items": order_items_rows,
        }
        
        # Write to PostgreSQL
        logger.info("Writing data to PostgreSQL...")
        for table_name, rows in tables.items():
            self.pg_writer.write_batch(table_name, rows)
        
        # Write to Azure (if available)
        if self.azure_writer:
            logger.info("Writing data to Azure Blob Storage...")
            for table_name, rows in tables.items():
                # Remove brz_ prefix for Azure blob naming
                clean_name = table_name.replace('brz_', '')
                blob_name = f"landing-zone/{clean_name}/{clean_name}_{timestamp_utc}.json"
                try:
                    self.azure_writer.write_events(rows, blob_name=blob_name)
                    logger.info(f"Uploaded {clean_name} to Azure")
                except Exception as e:
                    logger.error(f"Failed to upload {clean_name} to Azure: {e}")
        
        # Log statistics
        logger.info("\nData Generation Summary:")
        for table_name, rows in tables.items():
            logger.info(f"  {table_name}: {len(rows)} rows")
        
        logger.info(f"\nDistinct countries: {len({c['country'] for c in customers_rows})}")
        logger.info(f"Distinct cities: {len({c['city'] for c in customers_rows})}")
        logger.info(f"Distinct brands: {len(brands_rows)}")
    
    def run(self):
        """Run the event generator continuously."""
        self.initialize()
        
        logger.info(f"Event generator started. Will generate data every {EVENT_GENERATION_INTERVAL} seconds.")
        logger.info("Press Ctrl+C to stop.")
        
        try:
            while True:
                try:
                    self.generate_and_write_data()
                    logger.info(f"Sleeping for {EVENT_GENERATION_INTERVAL} seconds...")
                    time.sleep(EVENT_GENERATION_INTERVAL)
                except Exception as e:
                    logger.error(f"Error during data generation: {e}", exc_info=True)
                    logger.info("Waiting 30 seconds before retry...")
                    time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Shutting down event generator...")
        finally:
            self.pg_writer.close()


def main():
    """Main entry point."""
    generator = EventGenerator()
    generator.run()


if __name__ == "__main__":
    main()
