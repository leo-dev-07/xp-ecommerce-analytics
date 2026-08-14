#!/usr/bin/env python3
"""
Data Ingestion Script
Script to run the bronze layer data ingestion.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.bronze.data_ingestion import main as ingest_main

if __name__ == "__main__":
    try:
        ingest_main()
        print("Data ingestion completed successfully!")
    except Exception as e:
        print(f"Error during data ingestion: {str(e)}")
        sys.exit(1)