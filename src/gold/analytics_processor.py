"""
Gold Layer - Analytics Processing Module
Handles aggregation and business intelligence analytics.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict
import statistics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsProcessor:
    """Processes cleaned data to generate business intelligence insights."""
    
    def __init__(self):
        logger.info("Initialized AnalyticsProcessor")
    
    def aggregate_user_behavior(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate user behavior metrics.
        
        Args:
            events: List of cleaned event dictionaries
            
        Returns:
            Dictionary with user behavior analytics
        """
        user_metrics = defaultdict(list)
        session_data = defaultdict(list)
        
        # Group events by user and session
        for event in events:
            user_id = event["user_id"]
            session_id = event["session_id"]
            
            user_metrics[user_id].append(event)
            session_data[session_id].append(event)
        
        # Calculate metrics
        user_analytics = {}
        for user_id, user_events in user_metrics.items():
            user_analytics[user_id] = self._calculate_user_metrics(user_events)
        
        # Calculate session metrics
        session_analytics = {}
        for session_id, session_events in session_data.items():
            session_analytics[session_id] = self._calculate_session_metrics(session_events)
        
        return {
            "user_analytics": user_analytics,
            "session_analytics": session_analytics,
            "aggregated_at": datetime.now().isoformat()
        }
    
    def _calculate_user_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metrics for a specific user."""
        if not events:
            return {}
        
        # Count event types
        event_counts = defaultdict(int)
        total_events = len(events)
        
        for event in events:
            event_type = event["event_type"]
            event_counts[event_type] += 1
        
        # Calculate time-based metrics
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
        time_span = max(timestamps) - min(timestamps) if len(timestamps) > 1 else timedelta(0)
        
        # Calculate average session duration (simplified)
        avg_session_duration = 0  # In a real implementation, this would be calculated from session data
        
        return {
            "total_events": total_events,
            "event_distribution": dict(event_counts),
            "time_span_hours": time_span.total_seconds() / 3600,
            "avg_session_duration": avg_session_duration,
            "first_event": min(timestamps).isoformat(),
            "last_event": max(timestamps).isoformat()
        }
    
    def _calculate_session_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metrics for a specific session."""
        if not events:
            return {}
        
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
        time_span = max(timestamps) - min(timestamps) if len(timestamps) > 1 else timedelta(0)
        
        # Count event types
        event_counts = defaultdict(int)
        for event in events:
            event_type = event["event_type"]
            event_counts[event_type] += 1
        
        # Calculate conversion rate (if there are clicks and views)
        view_count = event_counts.get("product_view", 0)
        click_count = event_counts.get("product_click", 0)
        conversion_rate = click_count / view_count if view_count > 0 else 0
        
        return {
            "session_duration": time_span.total_seconds(),
            "total_events": len(events),
            "event_distribution": dict(event_counts),
            "conversion_rate": conversion_rate,
            "first_event": min(timestamps).isoformat(),
            "last_event": max(timestamps).isoformat()
        }
    
    def generate_product_analytics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate product-level analytics.
        
        Args:
            events: List of cleaned event dictionaries
            
        Returns:
            Dictionary with product analytics
        """
        product_metrics = defaultdict(list)
        
        # Group events by product
        for event in events:
            if "product_id" in event:
                product_metrics[event["product_id"]].append(event)
        
        # Calculate product metrics
        product_analytics = {}
        for product_id, product_events in product_metrics.items():
            product_analytics[product_id] = self._calculate_product_metrics(product_events)
        
        return {
            "product_analytics": product_analytics,
            "aggregated_at": datetime.now().isoformat()
        }
    
    def _calculate_product_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metrics for a specific product."""
        if not events:
            return {}
        
        # Count event types
        event_counts = defaultdict(int)
        total_events = len(events)
        
        for event in events:
            event_type = event["event_type"]
            event_counts[event_type] += 1
        
        # Calculate engagement metrics
        view_count = event_counts.get("product_view", 0)
        click_count = event_counts.get("product_click", 0)
        search_count = event_counts.get("search", 0)
        
        # Calculate conversion rate (clicks per view)
        conversion_rate = click_count / view_count if view_count > 0 else 0
        
        # Calculate average position for clicks
        positions = [e["position"] for e in events if "position" in e and e["event_type"] == "product_click"]
        avg_position = statistics.mean(positions) if positions else 0
        
        return {
            "total_events": total_events,
            "view_count": view_count,
            "click_count": click_count,
            "search_count": search_count,
            "conversion_rate": conversion_rate,
            "avg_position": avg_position,
            "event_distribution": dict(event_counts)
        }
    
    def generate_search_analytics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate search query analytics.
        
        Args:
            events: List of cleaned event dictionaries
            
        Returns:
            Dictionary with search analytics
        """
        search_queries = defaultdict(list)
        
        # Group events by search query
        for event in events:
            if event["event_type"] == "search":
                search_queries[event["query"]].append(event)
        
        # Calculate search metrics
        search_analytics = {}
        for query, query_events in search_queries.items():
            search_analytics[query] = self._calculate_search_metrics(query_events)
        
        return {
            "search_analytics": search_analytics,
            "aggregated_at": datetime.now().isoformat()
        }
    
    def _calculate_search_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate metrics for a specific search query."""
        if not events:
            return {}
        
        # Calculate average results count
        results_counts = [e["results_count"] for e in events]
        avg_results = statistics.mean(results_counts) if results_counts else 0
        
        # Calculate total searches and average time between searches
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
        time_span = max(timestamps) - min(timestamps) if len(timestamps) > 1 else timedelta(0)
        
        return {
            "total_searches": len(events),
            "avg_results_count": avg_results,
            "time_span_hours": time_span.total_seconds() / 3600,
            "first_search": min(timestamps).isoformat(),
            "last_search": max(timestamps).isoformat()
        }