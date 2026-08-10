"""
Bronze Layer - Data Ingestion Module
Handles raw data ingestion from Elasticsearch click streams.
"""

from abc import ABC, abstractmethod
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickStreamData(ABC):
    """Abstract base class for click stream data."""
    
    def __init__(self, event_type: str, timestamp: datetime, user_id: str):
        self.event_type = event_type
        self.timestamp = timestamp
        self.user_id = user_id
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert data to dictionary format."""
        pass


class ProductViewEvent(ClickStreamData):
    """Represents a product view event."""
    
    def __init__(self, timestamp: datetime, user_id: str, product_id: str, 
                 category: str, session_id: str):
        super().__init__("product_view", timestamp, user_id)
        self.product_id = product_id
        self.category = category
        self.session_id = session_id
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "product_id": self.product_id,
            "category": self.category,
            "session_id": self.session_id
        }


class ProductClickEvent(ClickStreamData):
    """Represents a product click event."""
    
    def __init__(self, timestamp: datetime, user_id: str, product_id: str, 
                 category: str, session_id: str, position: int):
        super().__init__("product_click", timestamp, user_id)
        self.product_id = product_id
        self.category = category
        self.session_id = session_id
        self.position = position
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "product_id": self.product_id,
            "category": self.category,
            "session_id": self.session_id,
            "position": self.position
        }


class SearchEvent(ClickStreamData):
    """Represents a search event."""
    
    def __init__(self, timestamp: datetime, user_id: str, query: str, 
                 session_id: str, results_count: int):
        super().__init__("search", timestamp, user_id)
        self.query = query
        self.session_id = session_id
        self.results_count = results_count
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "query": self.query,
            "session_id": self.session_id,
            "results_count": self.results_count
        }


class DataIngestor:
    """Handles ingestion of raw click stream data."""
    
    def __init__(self, data_source: str = "elasticsearch"):
        self.data_source = data_source
        logger.info(f"Initialized DataIngestor for {data_source}")
    
    def ingest_raw_data(self, raw_events: List[Dict[str, Any]]) -> List[ClickStreamData]:
        """
        Ingest raw click stream events and convert them to structured objects.
        
        Args:
            raw_events: List of raw event dictionaries
            
        Returns:
            List of structured ClickStreamData objects
        """
        structured_events = []
        
        for raw_event in raw_events:
            try:
                event_type = raw_event.get("event_type")
                
                if event_type == "product_view":
                    event = ProductViewEvent(
                        timestamp=datetime.fromisoformat(raw_event["timestamp"]),
                        user_id=raw_event["user_id"],
                        product_id=raw_event["product_id"],
                        category=raw_event["category"],
                        session_id=raw_event.get("session_id", "")
                    )
                elif event_type == "product_click":
                    event = ProductClickEvent(
                        timestamp=datetime.fromisoformat(raw_event["timestamp"]),
                        user_id=raw_event["user_id"],
                        product_id=raw_event["product_id"],
                        category=raw_event["category"],
                        session_id=raw_event.get("session_id", ""),
                        position=raw_event["position"]
                    )
                elif event_type == "search":
                    event = SearchEvent(
                        timestamp=datetime.fromisoformat(raw_event["timestamp"]),
                        user_id=raw_event["user_id"],
                        query=raw_event["query"],
                        session_id=raw_event.get("session_id", ""),
                        results_count=raw_event["results_count"]
                    )
                else:
                    logger.warning(f"Unknown event type: {event_type}")
                    continue
                
                structured_events.append(event)
                
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                continue
        
        logger.info(f"Ingested {len(structured_events)} events")
        return structured_events