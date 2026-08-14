#!/usr/bin/env python3
"""
Analytics Processing Script
Script to run the gold layer analytics processing.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.gold.analytics_processor import main as analytics_main

if __name__ == "__main__":
    try:
        analytics_main()
        print("Analytics processing completed successfully!")
    except Exception as e:
        print(f"Error during analytics processing: {str(e)}")
        sys.exit(1)