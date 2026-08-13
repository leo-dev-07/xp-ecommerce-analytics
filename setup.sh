#!/bin/bash

# Setup script for the data lake pipeline environment

echo "Setting up data lake pipeline environment..."

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install the packages in development mode
echo "Installing packages in development mode..."
pip install -e src/bronze
pip install -e src/silver
pip install -e src/gold

echo "Environment setup complete!"
echo ""
echo "To deploy to Databricks:"
echo "  databricks bundle deploy"
echo ""
echo "To run tests:"
echo "  pytest src/"
echo ""
echo "To start Jupyter notebook server:"
echo "  jupyter notebook"