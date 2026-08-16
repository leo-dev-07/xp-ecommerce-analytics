# File: src/scripts/generate_bronze_events.py
"""
Event Generation Script for E-commerce Analytics Pipeline (Bronze layer)

Generates synthetic data for the ecommerce.bronze schema and uploads each
table to its own landing-zone subdirectory in Azure Blob Storage:

    landing-zone/category/category_<ts>.json      -> brz_category
    landing-zone/brands/brands_<ts>.json           -> brz_brands
    landing-zone/customers/customers_<ts>.json     -> brz_customers
    landing-zone/calendar/calendar_<ts>.json       -> brz_calendar
    landing-zone/products/products_<ts>.json       -> brz_products
    landing-zone/order_items/order_items_<ts>.json -> brz_order_items

SCHEMA NOTE: only brz_brands, brz_category and brz_customers had confirmed
column lists available. brz_calendar, brz_products and brz_order_items were
not returned, so their columns are reasonable assumptions built from the
naming pattern of the confirmed tables. Update SCHEMA_DEFINITIONS / the
generator functions below if your real columns differ.

brz_customers also gets a "city" column here even though the confirmed
schema only listed customer_id/phone/country_code/country/state - you asked
for randomized cities, and there's nowhere else to put them. Delete the
"city" key in generate_customers_table() if your real table doesn't have it.

Lineage columns (_source_file, _ingested_at / ingested_at) are added by the
bronze ingestion job itself when it reads these landing files, so they are
NOT included in the generated JSON here.

VARIETY: countries/cities, brands, and product descriptions are all
generated from combinatorial reference data so that each of those exceeds
100 distinct values per run (see generate_products_table / generate_brands_table
/ CUSTOMER_LOCATIONS below).
"""

import random
import itertools
import json
import os
import shutil
from datetime import datetime, date, timedelta
from typing import List, Dict, Any

# Uses the fixed blob writer module - must be importable from this script's
# location (same directory, or on PYTHONPATH). Adjust the import path if
# azure_blob_writer.py lives elsewhere in your project structure.
from azure_blob_writer import AzureBlobEventWriter

datetime_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
credential = os.environ.get("AZURE_ACCESS_KEY")  # None => local-only run, upload skipped
container_name = "xp-project-ecommece"

# ---------------------------------------------------------------------------
# Schema reference (ecommerce.bronze). Columns marked (assumed) are inferred,
# not confirmed - see the module docstring above.
# ---------------------------------------------------------------------------
SCHEMA_DEFINITIONS = {
    "brz_category": {
        "category_code": "string",
        "category_name": "string",
    },
    "brz_brands": {
        "brand_code": "string",
        "brand_name": "string",
        "category_code": "string",
    },
    "brz_customers": {
        "customer_id": "string",
        "phone": "string",
        "country_code": "string",
        "country": "string",
        "state": "string",
        "city": "string",  # added - not in the confirmed schema, see note above
    },
    "brz_calendar": {  # assumed
        "date_code": "integer",       # YYYYMMDD
        "full_date": "date",
        "year": "integer",
        "quarter": "integer",
        "month": "integer",
        "month_name": "string",
        "day": "integer",
        "day_of_week": "integer",     # 1=Mon .. 7=Sun
        "day_name": "string",
        "is_weekend": "boolean",
    },
    "brz_products": {  # assumed
        "product_id": "string",
        "product_name": "string",
        "brand_code": "string",
        "category_code": "string",
        "price": "float",
        "description": "string",
    },
    "brz_order_items": {  # assumed
        "order_id": "string",
        "order_item_id": "string",
        "customer_id": "string",
        "product_id": "string",
        "date_code": "integer",
        "quantity": "integer",
        "unit_price": "float",
        "total_price": "float",
    },
}

# ---------------------------------------------------------------------------
# CATEGORIES - each entry drives its own products: nouns to build product
# names from, category-appropriate benefit phrases for descriptions, and a
# realistic price range.
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

# Nouns that are grammatically plural (e.g. "Trekking Poles"), so descriptions
# can use "these/they" instead of "a/an ... it" and read correctly.
PLURAL_NOUNS = {
    "Headphones", "Earbuds", "Running Shoes", "Resistance Bands", "Hiking Boots",
    "Trekking Poles", "Sneakers", "Cargo Pants", "Sunglasses", "Swim Trunks", "Ankle Boots",
}


def make_description(adj: str, noun: str, benefit: str) -> str:
    """Compose a grammatically-correct description, picking a/an and this/these
    correctly based on the adjective's first letter and whether the noun is plural."""
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

# Fictional brand-name building blocks (not real companies) - combined to
# produce 100+ unique brand names.
BRAND_PREFIXES = ["Nova", "Urban", "Prime", "Eco", "Swift", "Bright", "True", "Peak", "Vivid",
                   "Lumen", "Nimbus", "Solace", "Crest", "Orbit", "Vertex", "Harbor", "Meridian",
                   "Cobalt", "Ember", "Drift", "Alpine", "Cascade", "Fable", "Granite", "Halcyon",
                   "Ivory", "Juniper", "Kindle", "Lyric", "Mosaic", "Anchor", "Beacon", "Clover"]
BRAND_SUFFIXES = ["Corp", "Works", "Labs", "Co", "Hub", "Gear", "Craft", "Wave", "Line", "Group",
                   "Studio", "House", "Collective", "Supply", "Goods", "Forge", "Foundry",
                   "Society", "Union", "Traders"]

# ---------------------------------------------------------------------------
# CUSTOMER_LOCATIONS - real (city, state/region, country, country_code,
# calling_code) combinations spanning 60+ countries and 100+ cities, used to
# generate customers with realistic, varied geography and phone numbers.
# ---------------------------------------------------------------------------
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
    ("Phoenix", "Arizona", "United States", "US", "1"),
    ("Dallas", "Texas", "United States", "US", "1"),
    ("Philadelphia", "Pennsylvania", "United States", "US", "1"),
    ("Toronto", "Ontario", "Canada", "CA", "1"),
    ("Vancouver", "British Columbia", "Canada", "CA", "1"),
    ("Montreal", "Quebec", "Canada", "CA", "1"),
    ("Calgary", "Alberta", "Canada", "CA", "1"),
    ("Ottawa", "Ontario", "Canada", "CA", "1"),
    ("Mexico City", "Mexico City", "Mexico", "MX", "52"),
    ("Guadalajara", "Jalisco", "Mexico", "MX", "52"),
    ("Monterrey", "Nuevo Leon", "Mexico", "MX", "52"),
    ("Sao Paulo", "Sao Paulo", "Brazil", "BR", "55"),
    ("Rio de Janeiro", "Rio de Janeiro", "Brazil", "BR", "55"),
    ("Brasilia", "Federal District", "Brazil", "BR", "55"),
    ("Belo Horizonte", "Minas Gerais", "Brazil", "BR", "55"),
    ("Curitiba", "Parana", "Brazil", "BR", "55"),
    ("Buenos Aires", "Buenos Aires", "Argentina", "AR", "54"),
    ("Santiago", "Santiago Metropolitan", "Chile", "CL", "56"),
    ("Lima", "Lima", "Peru", "PE", "51"),
    ("Bogota", "Bogota", "Colombia", "CO", "57"),
    ("Medellin", "Antioquia", "Colombia", "CO", "57"),
    ("Quito", "Pichincha", "Ecuador", "EC", "593"),
    ("Montevideo", "Montevideo", "Uruguay", "UY", "598"),
    ("London", "England", "United Kingdom", "GB", "44"),
    ("Manchester", "England", "United Kingdom", "GB", "44"),
    ("Edinburgh", "Scotland", "United Kingdom", "GB", "44"),
    ("Cardiff", "Wales", "United Kingdom", "GB", "44"),
    ("Paris", "Ile-de-France", "France", "FR", "33"),
    ("Lyon", "Auvergne-Rhone-Alpes", "France", "FR", "33"),
    ("Marseille", "Provence-Alpes-Cote d'Azur", "France", "FR", "33"),
    ("Berlin", "Berlin", "Germany", "DE", "49"),
    ("Munich", "Bavaria", "Germany", "DE", "49"),
    ("Hamburg", "Hamburg", "Germany", "DE", "49"),
    ("Madrid", "Madrid", "Spain", "ES", "34"),
    ("Barcelona", "Catalonia", "Spain", "ES", "34"),
    ("Valencia", "Valencia", "Spain", "ES", "34"),
    ("Rome", "Lazio", "Italy", "IT", "39"),
    ("Milan", "Lombardy", "Italy", "IT", "39"),
    ("Naples", "Campania", "Italy", "IT", "39"),
    ("Amsterdam", "North Holland", "Netherlands", "NL", "31"),
    ("Rotterdam", "South Holland", "Netherlands", "NL", "31"),
    ("Brussels", "Brussels", "Belgium", "BE", "32"),
    ("Lisbon", "Lisbon", "Portugal", "PT", "351"),
    ("Porto", "Porto", "Portugal", "PT", "351"),
    ("Dublin", "Leinster", "Ireland", "IE", "353"),
    ("Stockholm", "Stockholm", "Sweden", "SE", "46"),
    ("Oslo", "Oslo", "Norway", "NO", "47"),
    ("Copenhagen", "Capital Region", "Denmark", "DK", "45"),
    ("Helsinki", "Uusimaa", "Finland", "FI", "358"),
    ("Reykjavik", "Capital Region", "Iceland", "IS", "354"),
    ("Warsaw", "Masovian", "Poland", "PL", "48"),
    ("Krakow", "Lesser Poland", "Poland", "PL", "48"),
    ("Prague", "Prague", "Czech Republic", "CZ", "420"),
    ("Vienna", "Vienna", "Austria", "AT", "43"),
    ("Zurich", "Zurich", "Switzerland", "CH", "41"),
    ("Geneva", "Geneva", "Switzerland", "CH", "41"),
    ("Athens", "Attica", "Greece", "GR", "30"),
    ("Budapest", "Budapest", "Hungary", "HU", "36"),
    ("Bucharest", "Bucharest", "Romania", "RO", "40"),
    ("Sofia", "Sofia", "Bulgaria", "BG", "359"),
    ("Zagreb", "Zagreb", "Croatia", "HR", "385"),
    ("Belgrade", "Belgrade", "Serbia", "RS", "381"),
    ("Bratislava", "Bratislava", "Slovakia", "SK", "421"),
    ("Ljubljana", "Ljubljana", "Slovenia", "SI", "386"),
    ("Vilnius", "Vilnius", "Lithuania", "LT", "370"),
    ("Riga", "Riga", "Latvia", "LV", "371"),
    ("Tallinn", "Harju", "Estonia", "EE", "372"),
    ("Kyiv", "Kyiv", "Ukraine", "UA", "380"),
    ("Istanbul", "Istanbul", "Turkey", "TR", "90"),
    ("Moscow", "Moscow", "Russia", "RU", "7"),
    ("Cairo", "Cairo", "Egypt", "EG", "20"),
    ("Casablanca", "Casablanca-Settat", "Morocco", "MA", "212"),
    ("Lagos", "Lagos", "Nigeria", "NG", "234"),
    ("Nairobi", "Nairobi", "Kenya", "KE", "254"),
    ("Accra", "Greater Accra", "Ghana", "GH", "233"),
    ("Addis Ababa", "Addis Ababa", "Ethiopia", "ET", "251"),
    ("Johannesburg", "Gauteng", "South Africa", "ZA", "27"),
    ("Cape Town", "Western Cape", "South Africa", "ZA", "27"),
    ("Dubai", "Dubai", "United Arab Emirates", "AE", "971"),
    ("Abu Dhabi", "Abu Dhabi", "United Arab Emirates", "AE", "971"),
    ("Riyadh", "Riyadh", "Saudi Arabia", "SA", "966"),
    ("Doha", "Doha", "Qatar", "QA", "974"),
    ("Kuwait City", "Al Asimah", "Kuwait", "KW", "965"),
    ("Amman", "Amman", "Jordan", "JO", "962"),
    ("Tel Aviv", "Tel Aviv", "Israel", "IL", "972"),
    ("Mumbai", "Maharashtra", "India", "IN", "91"),
    ("Delhi", "Delhi", "India", "IN", "91"),
    ("Bangalore", "Karnataka", "India", "IN", "91"),
    ("Chennai", "Tamil Nadu", "India", "IN", "91"),
    ("Hyderabad", "Telangana", "India", "IN", "91"),
    ("Kolkata", "West Bengal", "India", "IN", "91"),
    ("Pune", "Maharashtra", "India", "IN", "91"),
    ("Karachi", "Sindh", "Pakistan", "PK", "92"),
    ("Lahore", "Punjab", "Pakistan", "PK", "92"),
    ("Dhaka", "Dhaka", "Bangladesh", "BD", "880"),
    ("Colombo", "Western", "Sri Lanka", "LK", "94"),
    ("Kathmandu", "Bagmati", "Nepal", "NP", "977"),
    ("Bangkok", "Bangkok", "Thailand", "TH", "66"),
    ("Jakarta", "Jakarta", "Indonesia", "ID", "62"),
    ("Manila", "Metro Manila", "Philippines", "PH", "63"),
    ("Ho Chi Minh City", "Ho Chi Minh", "Vietnam", "VN", "84"),
    ("Hanoi", "Hanoi", "Vietnam", "VN", "84"),
    ("Singapore", "Singapore", "Singapore", "SG", "65"),
    ("Kuala Lumpur", "Kuala Lumpur", "Malaysia", "MY", "60"),
    ("Phnom Penh", "Phnom Penh", "Cambodia", "KH", "855"),
    ("Yangon", "Yangon", "Myanmar", "MM", "95"),
    ("Seoul", "Seoul", "South Korea", "KR", "82"),
    ("Busan", "Busan", "South Korea", "KR", "82"),
    ("Tokyo", "Tokyo", "Japan", "JP", "81"),
    ("Osaka", "Osaka", "Japan", "JP", "81"),
    ("Beijing", "Beijing", "China", "CN", "86"),
    ("Shanghai", "Shanghai", "China", "CN", "86"),
    ("Shenzhen", "Guangdong", "China", "CN", "86"),
    ("Guangzhou", "Guangdong", "China", "CN", "86"),
    ("Chengdu", "Sichuan", "China", "CN", "86"),
    ("Hong Kong", "Hong Kong", "Hong Kong", "HK", "852"),
    ("Taipei", "Taipei", "Taiwan", "TW", "886"),
    ("Sydney", "New South Wales", "Australia", "AU", "61"),
    ("Melbourne", "Victoria", "Australia", "AU", "61"),
    ("Brisbane", "Queensland", "Australia", "AU", "61"),
    ("Perth", "Western Australia", "Australia", "AU", "61"),
    ("Auckland", "Auckland", "New Zealand", "NZ", "64"),
    ("Wellington", "Wellington", "New Zealand", "NZ", "64"),
    ("Almaty", "Almaty", "Kazakhstan", "KZ", "7"),
    ("Tbilisi", "Tbilisi", "Georgia", "GE", "995"),
    ("Baku", "Baku", "Azerbaijan", "AZ", "994"),
]


def make_price(min_price: float, max_price: float) -> float:
    """Random, realistic-looking price within a range (e.g. ends in .99/.95/.49)."""
    raw = random.uniform(min_price, max_price)
    if raw < 20:
        return round(raw, 2)
    ending = random.choice([0.99, 0.95, 0.49, 0.00])
    return round(int(raw) + ending, 2)


def generate_category_table() -> List[Dict[str, Any]]:
    return [
        {"category_code": c["category_code"], "category_name": c["category_name"]}
        for c in CATEGORY_DEFINITIONS
    ]


def generate_brands_table(num_brands: int = 130) -> List[Dict[str, Any]]:
    """Generate num_brands unique fictional brand names, spread evenly across categories."""
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
        iso_weekday = current.isoweekday()  # 1=Mon .. 7=Sun
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
    """Generate products_per_category products for every category (>100 total
    with unique, randomly-composed descriptions and randomized prices)."""
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
        # New order roughly every 1-3 line items, so rows group into orders
        # instead of one order per row.
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


def cleanup_local_directory(directory: str):
    """
    Remove a local directory and everything in it.
    Only called after every table has uploaded successfully, so the local
    copy doesn't get deleted before the data is safely in Blob Storage.
    """
    if not directory or directory in (".", "/", ".."):
        print(f"Skipping cleanup: refusing to remove '{directory}'")
        return

    if os.path.isdir(directory):
        shutil.rmtree(directory)
        print(f"Removed local directory: {directory}")
    else:
        print(f"Nothing to clean up, directory not found: {directory}")


def save_table_to_file(rows: List[Dict[str, Any]], filename: str):
    """Save a table's rows to a local JSON file (backup/debugging copy)."""
    directory = os.path.dirname(filename)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(filename, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"Saved {len(rows)} rows to {filename}")


def main():
    """Generate synthetic data for every bronze table and upload each one to
    its own landing-zone subdirectory."""
    print("Generating E-commerce Bronze Layer Data...")

    category_rows = generate_category_table()
    brands_rows = generate_brands_table(num_brands=130)
    customers_rows = generate_customers_table(num_customers=300)
    calendar_rows = generate_calendar_table(date(2024, 1, 1), date(2026, 12, 31))
    products_rows = generate_products_table(brands_rows, products_per_category=20)
    order_items_rows = generate_order_items_table(
        customers_rows, products_rows, calendar_rows, num_order_items=3000
    )

    tables = {
        "category": category_rows,
        "brands": brands_rows,
        "customers": customers_rows,
        "calendar": calendar_rows,
        "products": products_rows,
        "order_items": order_items_rows,
    }

    local_root = f"{container_name}/landing-zone"

    blob_writer = None
    if credential:
        try:
            blob_writer = AzureBlobEventWriter(container_name=container_name, credential=credential)
        except Exception as e:
            print(f"Warning: could not initialize Azure Blob writer: {e}")
    else:
        print("AZURE_ACCESS_KEY not set - generating local files only, skipping upload.")

    all_uploads_succeeded = True
    for table_name, rows in tables.items():
        local_path = f"{local_root}/{table_name}/{table_name}.json"
        save_table_to_file(rows, local_path)

        if blob_writer is not None:
            blob_name = f"landing-zone/{table_name}/{table_name}_{datetime_utc}.json"
            try:
                blob_url = blob_writer.write_events(rows, blob_name=blob_name)
                print(f"Uploaded {table_name} -> {blob_url}")
            except Exception as e:
                all_uploads_succeeded = False
                print(f"Warning: failed to upload {table_name} to Azure Blob Storage: {e}")
        else:
            all_uploads_succeeded = False

    print("\nRow counts:")
    for table_name, rows in tables.items():
        print(f"  {table_name}: {len(rows)}")

    print(f"\nDistinct countries: {len({c['country'] for c in customers_rows})}")
    print(f"Distinct cities: {len({c['city'] for c in customers_rows})}")
    print(f"Distinct brands: {len(brands_rows)}")
    print(f"Distinct product descriptions: {len({p['description'] for p in products_rows})}")

    if all_uploads_succeeded:
        cleanup_local_directory(local_root)
    else:
        print(f"\nKeeping local files in {local_root} since not every table uploaded successfully.")


if __name__ == "__main__":
    main()