"""
Unit tests for data transformation components.
"""

import unittest
from src.silver.data_transformer import DataTransformer
from src.bronze.data_ingestion import (
    ProductViewEvent,
    ProductClickEvent,
    SearchEvent
)

class TestDataTransformer(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.transformer = DataTransformer()
    
    def test_clean_events_with_valid_data(self):
        """Test cleaning of valid events."""
        # Create some sample events
        view_event = ProductViewEvent(
            timestamp="2023-01-01T10:00:00Z",
            user_id="user_123",
            product_id="prod_456",
            category="electronics",
            session_id="session_789"
        )
        
        click_event = ProductClickEvent(
            timestamp="2023-01-01T10:05:00Z",
            user_id="user_123",
            product_id="prod_456",
            category="electronics",
            session_id="session_789",
            position=3
        )
        
        events = [view_event, click_event]
        cleaned_events = self.transformer.clean_events(events)
        
        self.assertEqual(len(cleaned_events), 2)
        self.assertIn("processed_at", cleaned_events[0])
    
    def test_enrich_events_with_conversion(self):
        """Test enrichment of events with conversion indicators."""
        # Create a sample event
        click_event = ProductClickEvent(
            timestamp="2023-01-01T10:05:00Z",
            user_id="user_123",
            product_id="prod_456",
            category="electronics",
            session_id="session_789",
            position=3
        )
        
        # Convert to dictionary format for enrichment
        event_dict = {
            "event_type": "product_click",
            "timestamp": "2023-01-01T10:05:00Z",
            "user_id": "user_123",
            "product_id": "prod_456",
            "category": "electronics",
            "session_id": "session_789",
            "position": 3
        }
        
        enriched_events = self.transformer.enrich_events([event_dict])
        
        self.assertTrue(enriched_events[0]["is_conversion"])

if __name__ == '__main__':
    unittest.main()