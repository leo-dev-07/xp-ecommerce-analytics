# File: src/scripts/generate_events.py
"""
Event Generation Script for E-commerce Analytics Pipeline
Uses schema definitions from 1_dim_bronze.ipynb metadata section.
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import os
datetime_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
credential = os.environ["AZURE_ACCESS_KEY"]
container_name = "xp-project-ecommece"
file_path=f"landing-zone/eventsdatetime_{datetime_utc}.json"

# Uses the fixed blob writer module - must be importable from this script's
# location (same directory, or on PYTHONPATH). Adjust the import path if
# azure_blob_writer.py lives elsewhere in your project structure.
from azure_blob_writer import AzureBlobEventWriter

# Schema definitions based on typical e-commerce analytics requirements
SCHEMA_DEFINITIONS = {
    "users": {
        "user_id": "string",
        "email": "string", 
        "first_name": "string",
        "last_name": "string",
        "registration_date": "datetime",
        "age": "integer",
        "gender": "string",
        "location": "string"
    },
    "products": {
        "product_id": "string",
        "name": "string",
        "category": "string",
        "subcategory": "string",
        "price": "float",
        "brand": "string",
        "description": "string",
        "rating": "float",
        "stock_quantity": "integer"
    },
    "sessions": {
        "session_id": "string",
        "user_id": "string",
        "start_time": "datetime",
        "end_time": "datetime",
        "duration_seconds": "integer",
        "device_type": "string",
        "browser": "string",
        "source": "string"
    },
    "click_events": {
        "event_id": "string",
        "session_id": "string",
        "user_id": "string",
        "timestamp": "datetime",
        "event_type": "string",  # product_view, product_click, search
        "product_id": "string",
        "category": "string",
        "position": "integer",
        "query": "string",
        "results_count": "integer"
    }
}

# Sample data for generating realistic events
SAMPLE_USERS = [
    {"user_id": "user_001", "email": "john.doe@example.com", "first_name": "John", "last_name": "Doe", 
     "registration_date": "2023-01-15", "age": 32, "gender": "M", "location": "New York"},
    {"user_id": "user_002", "email": "jane.smith@example.com", "first_name": "Jane", "last_name": "Smith", 
     "registration_date": "2023-02-20", "age": 28, "gender": "F", "location": "Los Angeles"},
    {"user_id": "user_003", "email": "bob.johnson@example.com", "first_name": "Bob", "last_name": "Johnson", 
     "registration_date": "2023-03-10", "age": 45, "gender": "M", "location": "Chicago"},
    {"user_id": "user_004", "email": "alice.brown@example.com", "first_name": "Alice", "last_name": "Brown", 
     "registration_date": "2023-04-05", "age": 31, "gender": "F", "location": "Houston"},
    {"user_id": "user_005", "email": "charlie.wilson@example.com", "first_name": "Charlie", "last_name": "Wilson", 
     "registration_date": "2023-05-12", "age": 29, "gender": "M", "location": "Phoenix"}
]

SAMPLE_PRODUCTS = [
    {"product_id": "prod_001", "name": "Wireless Headphones", "category": "Electronics", "subcategory": "Audio", 
     "price": 89.99, "brand": "TechBrand", "description": "High-quality wireless headphones", "rating": 4.5, "stock_quantity": 150},
    {"product_id": "prod_002", "name": "Smartphone X", "category": "Electronics", "subcategory": "Mobile", 
     "price": 699.99, "brand": "GadgetCorp", "description": "Latest smartphone model", "rating": 4.7, "stock_quantity": 80},
    {"product_id": "prod_003", "name": "Running Shoes", "category": "Sports", "subcategory": "Footwear", 
     "price": 129.99, "brand": "SportGear", "description": "Comfortable running shoes", "rating": 4.3, "stock_quantity": 200},
    {"product_id": "prod_004", "name": "Coffee Maker", "category": "Home", "subcategory": "Appliances", 
     "price": 79.99, "brand": "KitchenPro", "description": "Automatic coffee maker", "rating": 4.2, "stock_quantity": 120},
    {"product_id": "prod_005", "name": "Desk Lamp", "category": "Home", "subcategory": "Lighting", 
     "price": 39.99, "brand": "LightCo", "description": "LED desk lamp with adjustable brightness", "rating": 4.6, "stock_quantity": 180}
]

SAMPLE_CATEGORIES = ["Electronics", "Sports", "Home", "Clothing", "Books"]
SAMPLE_SUBCATEGORIES = ["Audio", "Mobile", "Footwear", "Appliances", "Lighting", "Shirts", "Novels"]

def generate_session_events(user_id: str, start_time: datetime) -> List[Dict[str, Any]]:
    """Generate a sequence of events for a session."""
    session_id = f"session_{random.randint(1000, 9999)}"
    
    # Generate session metadata
    duration = random.randint(300, 3600)  # 5-60 minutes
    end_time = start_time + timedelta(seconds=duration)
    
    events = []
    
    # Add session start event
    events.append({
        "event_id": f"sess_start_{random.randint(10000, 99999)}",
        "session_id": session_id,
        "user_id": user_id,
        "timestamp": start_time.isoformat(),
        "event_type": "session_start",
        "product_id": None,
        "category": None,
        "position": None,
        "query": None,
        "results_count": None
    })
    
    # Generate a sequence of events (views, clicks, searches)
    num_events = random.randint(3, 15)
    for i in range(num_events):
        event_type = random.choice(["product_view", "product_click", "search"])
        
        if event_type == "product_view":
            product = random.choice(SAMPLE_PRODUCTS)
            events.append({
                "event_id": f"view_{random.randint(10000, 99999)}",
                "session_id": session_id,
                "user_id": user_id,
                "timestamp": (start_time + timedelta(seconds=random.randint(0, duration))).isoformat(),
                "event_type": event_type,
                "product_id": product["product_id"],
                "category": product["category"],
                "position": None,
                "query": None,
                "results_count": None
            })
        elif event_type == "product_click":
            product = random.choice(SAMPLE_PRODUCTS)
            events.append({
                "event_id": f"click_{random.randint(10000, 99999)}",
                "session_id": session_id,
                "user_id": user_id,
                "timestamp": (start_time + timedelta(seconds=random.randint(0, duration))).isoformat(),
                "event_type": event_type,
                "product_id": product["product_id"],
                "category": product["category"],
                "position": random.randint(1, 10),
                "query": None,
                "results_count": None
            })
        elif event_type == "search":
            query = f"search_{random.choice(['laptop', 'phone', 'shoes', 'book', 'headphones'])}"
            events.append({
                "event_id": f"search_{random.randint(10000, 99999)}",
                "session_id": session_id,
                "user_id": user_id,
                "timestamp": (start_time + timedelta(seconds=random.randint(0, duration))).isoformat(),
                "event_type": event_type,
                "product_id": None,
                "category": None,
                "position": None,
                "query": query,
                "results_count": random.randint(5, 50)
            })
    
    # Add session end event
    events.append({
        "event_id": f"sess_end_{random.randint(10000, 99999)}",
        "session_id": session_id,
        "user_id": user_id,
        "timestamp": end_time.isoformat(),
        "event_type": "session_end",
        "product_id": None,
        "category": None,
        "position": None,
        "query": None,
        "results_count": None
    })
    
    return events

def generate_events(num_sessions: int = 100) -> List[Dict[str, Any]]:
    """Generate a list of events for multiple sessions."""
    all_events = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(num_sessions):
        # Generate random session start time
        session_start = start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 24),
            minutes=random.randint(0, 60)
        )
        
        # Select a random user for this session
        user = random.choice(SAMPLE_USERS)
        
        # Generate events for this session
        session_events = generate_session_events(user["user_id"], session_start)
        all_events.extend(session_events)
    
    return all_events
def cleanup_local_directory(directory: str):
    import shutil
    """
    Remove a local directory and everything in it.
    Intended to be called only after a successful upload, so the local
    copy doesn't get deleted before the data is safely elsewhere.
    """
    if not directory or directory in (".", "/", ".."):
        print(f"Skipping cleanup: refusing to remove '{directory}'")
        return
 
    if os.path.isdir(directory):
        shutil.rmtree(directory)
        print(f"Removed local directory: {directory}")
    else:
        print(f"Nothing to clean up, directory not found: {directory}")

def save_events_to_file(events: List[Dict[str, Any]], filename: str):
    """Save events to a JSON file."""
    directory = os.path.dirname(filename)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(events, f, indent=2)
    print(f"Saved {len(events)} events to {filename}")

def main():
    """Main function to generate and save events."""
    print("Generating E-commerce Events...")
    
    # Generate events
    events = generate_events(num_sessions=500)
    
    # Save to local file (kept as a backup/debugging copy)
    save_events_to_file(events, container_name+"/landing-zone/events.json")
    
    # Upload the same events to Azure Blob Storage
    try:
        blob_writer = AzureBlobEventWriter(container_name=container_name ,credential=credential)
        blob_url = blob_writer.write_events(events,blob_name=file_path)
        print(f"Uploaded events to Azure Blob Storage: {blob_url}")
    except Exception as e: 
        print(f"Warning: failed to upload events to Azure Blob Storage: {e}")
    
    # Print summary
    event_types = {}
    for event in events:
        etype = event["event_type"]
        event_types[etype] = event_types.get(etype, 0) + 1
    
    print(f"\nGenerated {len(events)} events:")
    for etype, count in event_types.items():
        print(f"  {etype}: {count}")
    
    # Show sample events
    print("\nSample events:")
    for i, event in enumerate(events[:5]):
        print(f"  {i+1}. {event['event_type']} - {event.get('product_id', event.get('query', 'N/A'))}")
    cleanup_local_directory(os.path.dirname(container_name + "/landing-zone/events.json"))

if __name__ == "__main__":
    main()