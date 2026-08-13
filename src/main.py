#!/usr/bin/env python3
"""
Main entry point for E-commerce Analytics Pipeline.
"""

import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from bronze.data_ingestion import DataIngestor, ProductViewEvent, ProductClickEvent, SearchEvent
from silver.data_transformer import DataTransformer
from gold.analytics_processor import AnalyticsProcessor

def main():
    """Demonstrate the complete pipeline usage."""
    print("E-commerce Analytics Pipeline Demo")
    print("=" * 40)
    
    # Step 1: Create sample events (these would come from your data source)
    print("\n1. Creating sample events...")
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
    
    # Step 2: Bronze Layer - Data Ingestion
    print("\n2. Bronze Layer - Data Ingestion...")
    ingestor = DataIngestor()
    raw_events = [event.to_dict() for event in events]
    structured_events = ingestor.ingest_raw_data(raw_events)
    print(f"   Ingested {len(structured_events)} events")
    
    # Step 3: Silver Layer - Data Transformation
    print("\n3. Silver Layer - Data Transformation...")
    transformer = DataTransformer()
    # Pass the ClickStreamData objects directly to clean_events (not dictionaries)
    cleaned_events = transformer.clean_events(structured_events)  
    enriched_events = transformer.enrich_events(cleaned_events)
    print(f"   Transformed {len(enriched_events)} events")
    
    # Step 4: Gold Layer - Analytics Processing
    print("\n4. Gold Layer - Analytics Processing...")
    processor = AnalyticsProcessor()
    user_analytics = processor.aggregate_user_behavior(enriched_events)
    product_analytics = processor.generate_product_analytics(enriched_events)
    
    print("   User Analytics:")
    for key, value in user_analytics.items():
        if key != 'user_analytics' and key != 'session_analytics':
            print(f"     {key}: {value}")
    
    print("   Product Analytics:")
    for key, value in product_analytics.items():
        if key != 'product_analytics':
            print(f"     {key}: {value}")
    
    print("\nPipeline demo completed successfully!")

if __name__ == "__main__":
    main()