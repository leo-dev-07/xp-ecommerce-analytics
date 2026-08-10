#!/usr/bin/env python3
"""
Event Generation Script for E-commerce Analytics
Generates test click stream events for the analytics pipeline.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample data for generating events
PRODUCTS = [
    "product_001", "product_002", "product_003", "product_004", "product_005",
    "product_006", "product_007", "product_008", "product_009", "product_010"
]

CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]

USERS = [f"user_{i:04d}" for i in range(1, 101)]  # 100 users

SESSIONS = [f"session_{i:06d}" for i in range(1, 501)]  # 500 sessions


def generate_product_view_event() -> Dict[str, Any]:
    """Generate a product view event."""
    return {
        "event_type": "product_view",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "session_id": random.choice(SESSIONS)
    }


def generate_product_click_event() -> Dict[str, Any]:
    """Generate a product click event."""
    return {
        "event_type": "product_click",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "session_id": random.choice(SESSIONS),
        "position": random.randint(1, 20)
    }


def generate_search_event() -> Dict[str, Any]:
    """Generate a search event."""
    return {
        "event_type": "search",
        "timestamp": datetime.now().isoformat(),
        "user_id": random.choice(USERS),
        "query": f"search_query_{random.randint(1, 100)}",
        "session_id": random.choice(SESSIONS),
        "results_count": random.randint(0, 50)
    }


def generate_events(count: int = 100) -> List[Dict[str, Any]]:
    """
    Generate a list of click stream events.
    
    Args:
        count: Number of events to generate
        
    Returns:
        List of event dictionaries
    """
    events = []
    
    for _ in range(count):
        # Randomly choose event type (70% views, 25% clicks, 5% searches)
        event_type = random.choices(
            ["product_view", "product_click", "search"],
            weights=[0.7, 0.25, 0.05]
        )[0]
        
        if event_type == "product_view":
            event = generate_product_view_event()
        elif event_type == "product_click":
            event = generate_product_click_event()
        else:  # search
            event = generate_search_event()
        
        events.append(event)
    
    return events


def save_events_to_file(events: List[Dict[str, Any]], filename: str):
    """
    Save events to a JSON file.
    
    Args:
        events: List of event dictionaries
        filename: Output filename
    """
    try:
        with open(filename, 'w') as f:
            json.dump(events, f, indent=2)
        logger.info(f"Saved {len(events)} events to {filename}")
    except Exception as e:
        logger.error(f"Error saving events to file: {e}")


def main():
    """Main function to generate and save events."""
    logger.info("Starting event generation...")
    
    # Generate events
    events = generate_events(500)
    
    # Save to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/click_stream_events_{timestamp}.json"
    
    save_events_to_file(events, filename)
    
    logger.info("Event generation completed successfully!")


if __name__ == "__main__":
    main()