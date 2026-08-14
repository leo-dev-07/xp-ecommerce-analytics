"""
Unit tests for data ingestion components.
"""

import unittest
from src.bronze.data_ingestion import (
    DataIngestor,
    ProductViewEvent,
    ProductClickEvent,
    SearchEvent
)

class TestDataIngestion(unittest.TestCase):
    
    def test_product_view_event_creation(self):
        """Test creation of ProductViewEvent."""
        event = ProductViewEvent(
            timestamp="2023-01-01T10:00:00Z",
            user_id="user_123",
            product_id="prod_456",
            category="electronics",
            session_id="session_789"
        )
        
        self.assertEqual(event.event_type, "product_view")
        self.assertEqual(event.user_id, "user_123")
        self.assertEqual(event.product_id, "prod_456")
    
    def test_product_click_event_creation(self):
        """Test creation of ProductClickEvent."""
        event = ProductClickEvent(
            timestamp="2023-01-01T10:00:00Z",
            user_id="user_123",
            product_id="prod_456",
            category="electronics",
            session_id="session_789",
            position=3
        )
        
        self.assertEqual(event.event_type, "product_click")
        self.assertEqual(event.position, 3)
    
    def test_search_event_creation(self):
        """Test creation of SearchEvent."""
        event = SearchEvent(
            timestamp="2023-01-01T10:00:00Z",
            user_id="user_123",
            query="laptop",
            session_id="session_789",
            results_count=24
        )
        
        self.assertEqual(event.event_type, "search")
        self.assertEqual(event.query, "laptop")

if __name__ == '__main__':
    unittest.main()