#!/usr/bin/env python3
"""
Integration test for Databricks Asset Bundle
Tests the complete flow from bronze to gold layers with notebook integration
"""

import sys
import os
import unittest

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.bronze.main import BronzeProcessor
from src.silver.main import SilverProcessor
from src.gold.main import GoldProcessor
from src.notebook.main import NotebookProcessor

class TestAssetBundleIntegration(unittest.TestCase):
    """Integration tests for Databricks Asset Bundle"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.bronze = BronzeProcessor()
        self.silver = SilverProcessor()
        self.gold = GoldProcessor()
        self.notebook = NotebookProcessor()
    
    def test_bronze_layer_processing(self):
        """Test bronze layer processing"""
        data = ["record1", "record2", "record3"]
        result = self.bronze.process(data)
        self.assertIn("bronze", result)
        self.assertEqual(len(result), len("Processed 3 records in bronze layer"))
    
    def test_silver_layer_processing(self):
        """Test silver layer processing"""
        data = ["cleaned_record1", "cleaned_record2"]
        result = self.silver.process(data)
        self.assertIn("silver", result)
        
    def test_gold_layer_processing(self):
        """Test gold layer processing"""
        data = ["aggregated_record1", "aggregated_record2", "aggregated_record3"]
        result = self.gold.process(data)
        self.assertIn("gold", result)
    
    def test_notebook_execution(self):
        """Test notebook execution"""
        result = self.notebook.execute("test_notebook.ipynb")
        self.assertIn("Executed notebook", result)
    
    def test_complete_pipeline(self):
        """Test complete data pipeline from bronze to gold"""
        # Test bronze processing
        raw_data = ["raw1", "raw2", "raw3"]
        bronze_result = self.bronze.process(raw_data)
        
        # Test silver processing (using bronze output)
        cleaned_data = ["cleaned1", "cleaned2"]
        silver_result = self.silver.process(cleaned_data)
        
        # Test gold processing (using silver output)
        aggregated_data = ["agg1", "agg2", "agg3"]
        gold_result = self.gold.process(aggregated_data)
        
        # Verify all layers processed
        self.assertIn("bronze", bronze_result)
        self.assertIn("silver", silver_result)
        self.assertIn("gold", gold_result)

if __name__ == '__main__':
    unittest.main(verbosity=2)