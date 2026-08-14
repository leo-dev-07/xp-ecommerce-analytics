#!/usr/bin/env python3
"""
Data Transformation Script
Script to run the silver layer data transformation.
"""

import sys
import os

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.silver.data_transformer import main as transform_main

if __name__ == "__main__":
    try:
        transform_main()
        print("Data transformation completed successfully!")
    except Exception as e:
        print(f"Error during data transformation: {str(e)}")
        sys.exit(1)