#!/usr/bin/env python3
"""
Test script to verify the E-commerce Analytics pipeline components work correctly.
"""

import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from bronze.data_ingestion import DataIngestor, ProductViewEvent, ProductClickEvent, SearchEvent, ClickStreamData
from silver.data_transformer import DataTransformer
from gold.analytics_processor import AnalyticsProcessor

def test_bronze_layer():
    """Test the bronze layer components."""
    print("Testing Bronze Layer...")
    
    # Create some sample events using datetime objects
    events = [
        ProductViewEvent(
            timestamp=datetime.fromisoformat("2023-01-01T10:00:00"),
            user_id="user_001",
            product_id="product_001",
            category="Electronics",
            session_id="session_001"
        ),
        ProductClickEvent(
            timestamp=datetime.fromisoformat("2023-01-01T10:05:00"),
            user_id="user_001",
            product_id="product_001",
            category="Electronics",
            session_id="session_001",
            position=3
        ),
        SearchEvent(
            timestamp=datetime.fromisoformat("2023-01-01T10:10:00"),
            user_id="user_001",
            query="laptop",
            session_id="session_001",
            results_count=15
        )
    ]
    
    # Test data ingestion
    ingestor = DataIngestor()
    raw_events = [event.to_dict() for event in events]
    structured_events = ingestor.ingest_raw_data(raw_events)
    
    print(f"Successfully ingested {len(structured_events)} events")
    return True


def test_silver_layer():
    """Test the silver layer components."""
    print("Testing Silver Layer...")
    
    # Create some sample events using the ClickStreamData objects
    events = [
        ProductViewEvent(
            timestamp=datetime.fromisoformat("2023-01-01T10:00:00"),
            user_id="user_001",
            product_id="product_001",
            category="Electronics",
            session_id="session_001"
        ),
        ProductClickEvent(
            timestamp=datetime.fromisoformat("2023-01-01T10:05:00"),
            user_id="user_001",
            product_id="product_001",
            category="Electronics",
            session_id="session_001",
            position=3
        )
    ]
    
    # Test data transformation
    transformer = DataTransformer()
    cleaned_events = transformer.clean_events(events)
    enriched_events = transformer.enrich_events(cleaned_events)
    
    print(f"Successfully transformed {len(enriched_events)} events")
    return True


def test_gold_layer():
    """Test the gold layer components."""
    print("Testing Gold Layer...")
    
    # Create some sample events
    events = [
        {
            "event_type": "product_view",
            "timestamp": "2023-01-01T10:00:00",
            "user_id": "user_001",
            "product_id": "product_001",
            "category": "Electronics",
            "session_id": "session_001",
            "processed_at": "2023-01-01T10:01:00"
        },
        {
            "event_type": "product_click",
            "timestamp": "2023-01-01T10:05:00",
            "user_id": "user_001",
            "product_id": "product_001",
            "category": "Electronics",
            "session_id": "session_001",
            "position": 3,
            "processed_at": "2023-01-01T10:06:00"
        }
    ]
    
    # Test analytics processing
    processor = AnalyticsProcessor()
    user_analytics = processor.aggregate_user_behavior(events)
    product_analytics = processor.generate_product_analytics(events)
    
    print("Successfully processed analytics")
    print(f"User analytics keys: {list(user_analytics.keys())}")
    print(f"Product analytics keys: {list(product_analytics.keys())}")
    return True


def main():
    """Run all tests."""
    print("Running E-commerce Analytics Pipeline Tests...")
    print("=" * 50)
    
    try:
        test_bronze_layer()
        test_silver_layer()
        test_gold_layer()
        
        print("=" * 50)
        print("All tests passed successfully!")
        return 0
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())