"""
Silver Layer - Data Transformation Module
Handles cleaning and transformation of click stream data.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from bronze.data_ingestion import ClickStreamData, ProductViewEvent, ProductClickEvent, SearchEvent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data cleaning and transformation for click stream events."""
    
    def __init__(self):
        logger.info("Initialized DataTransformer")
    
    def clean_events(self, events: List[ClickStreamData]) -> List[Dict[str, Any]]:
        """
        Clean and validate click stream events.
        
        Args:
            events: List of ClickStreamData objects
            
        Returns:
            List of cleaned event dictionaries
        """
        cleaned_events = []
        
        for event in events:
            try:
                # Validate required fields
                if not self._validate_event(event):
                    logger.warning(f"Skipping invalid event: {event}")
                    continue
                
                # Transform the event
                cleaned_event = self._transform_event(event)
                cleaned_events.append(cleaned_event)
                
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                continue
        
        logger.info(f"Cleaned {len(cleaned_events)} events")
        return cleaned_events
    
    def _validate_event(self, event: ClickStreamData) -> bool:
        """Validate that an event has all required fields."""
        if not event.timestamp or not event.user_id:
            return False
        
        # Additional validation based on event type
        if isinstance(event, ProductViewEvent):
            return all([
                event.product_id,
                event.category,
                event.session_id
            ])
        elif isinstance(event, ProductClickEvent):
            return all([
                event.product_id,
                event.category,
                event.session_id,
                event.position is not None
            ])
        elif isinstance(event, SearchEvent):
            return all([
                event.query,
                event.session_id,
                event.results_count is not None
            ])
        
        return True
    
    def _transform_event(self, event: ClickStreamData) -> Dict[str, Any]:
        """Transform an event into a standardized format."""
        base_data = {
            "event_type": event.event_type,
            "timestamp": event.timestamp.isoformat(),
            "user_id": event.user_id,
            "processed_at": datetime.now().isoformat()
        }
        
        # Add type-specific fields
        if isinstance(event, ProductViewEvent):
            base_data.update({
                "product_id": event.product_id,
                "category": event.category,
                "session_id": event.session_id
            })
        elif isinstance(event, ProductClickEvent):
            base_data.update({
                "product_id": event.product_id,
                "category": event.category,
                "session_id": event.session_id,
                "position": event.position
            })
        elif isinstance(event, SearchEvent):
            base_data.update({
                "query": event.query,
                "session_id": event.session_id,
                "results_count": event.results_count
            })
        
        return base_data
    
    def enrich_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich events with additional metadata.
        
        Args:
            events: List of event dictionaries
            
        Returns:
            List of enriched event dictionaries
        """
        enriched_events = []
        
        for event in events:
            try:
                # Add session duration if it's a product view or click
                if event["event_type"] in ["product_view", "product_click"]:
                    # In a real implementation, this would be calculated from session data
                    event["session_duration"] = 0
                
                # Add time-based metadata
                timestamp = datetime.fromisoformat(event["timestamp"])
                event["hour_of_day"] = timestamp.hour
                event["day_of_week"] = timestamp.weekday()
                
                # Add user behavior indicators
                if event["event_type"] == "product_click":
                    event["is_conversion"] = True
                else:
                    event["is_conversion"] = False
                
                enriched_events.append(event)
                
            except Exception as e:
                logger.error(f"Error enriching event: {e}")
                continue
        
        logger.info(f"Enriched {len(enriched_events)} events")
        return enriched_events