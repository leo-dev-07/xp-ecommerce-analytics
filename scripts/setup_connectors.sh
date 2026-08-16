#!/bin/bash
# Setup script for Kafka Connect Connectors

set -e

echo "================================================"
echo "Kafka Connect - E-commerce Connectors Setup"
echo "================================================"
echo ""

# Configuration
CONNECT_URL="http://localhost:8083"
WAIT_TIME=5

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to wait for Kafka Connect to be ready
wait_for_connect() {
    echo -e "${YELLOW}Waiting for Kafka Connect to be ready...${NC}"
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -sf "${CONNECT_URL}" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ Kafka Connect is ready${NC}"
            return 0
        fi
        
        attempt=$((attempt + 1))
        echo "Attempt $attempt/$max_attempts - waiting..."
        sleep 5
    done
    
    echo -e "${RED}✗ Kafka Connect did not become ready in time${NC}"
    exit 1
}

# Function to delete a connector if it exists
delete_connector_if_exists() {
    connector_name=$1
    
    if curl -sf "${CONNECT_URL}/connectors/${connector_name}" > /dev/null 2>&1; then
        echo -e "${YELLOW}Deleting existing connector: ${connector_name}${NC}"
        curl -X DELETE "${CONNECT_URL}/connectors/${connector_name}"
        sleep 2
        echo -e "${GREEN}✓ Deleted${NC}"
    fi
}

# Function to create a connector
create_connector() {
    config_file=$1
    connector_name=$2
    
    echo ""
    echo -e "${YELLOW}Creating connector: ${connector_name}${NC}"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}✗ Config file not found: ${config_file}${NC}"
        return 1
    fi
    
    # Delete if exists
    delete_connector_if_exists "$connector_name"
    
    # Create connector
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data @"$config_file" \
        "${CONNECT_URL}/connectors")
    
    if echo "$response" | grep -q "error"; then
        echo -e "${RED}✗ Failed to create connector${NC}"
        echo "$response" | jq '.' 2>/dev/null || echo "$response"
        return 1
    else
        echo -e "${GREEN}✓ Connector created successfully${NC}"
        sleep $WAIT_TIME
    fi
}

# Function to check connector status
check_connector_status() {
    connector_name=$1
    
    echo ""
    echo -e "${YELLOW}Checking status: ${connector_name}${NC}"
    
    status=$(curl -s "${CONNECT_URL}/connectors/${connector_name}/status")
    
    if echo "$status" | grep -q '"state":"RUNNING"'; then
        echo -e "${GREEN}✓ Status: RUNNING${NC}"
        echo "$status" | jq '.connector.state, .tasks[].state' 2>/dev/null || echo "$status"
    else
        echo -e "${RED}✗ Status: NOT RUNNING${NC}"
        echo "$status" | jq '.' 2>/dev/null || echo "$status"
    fi
}

# Main execution
main() {
    echo "Starting setup process..."
    echo ""
    
    # Wait for Kafka Connect
    wait_for_connect
    
    echo ""
    echo "================================================"
    echo "Step 1: Creating JDBC Source Connector"
    echo "================================================"
    
    # Create PostgreSQL source connector
    create_connector \
        "connectors/source/connector_brz_all_tables.json" \
        "postgres-source-ecommerce-all"
    
    check_connector_status "postgres-source-ecommerce-all"
    
    echo ""
    echo "================================================"
    echo "Step 2: Creating Azure Blob Sink Connector"
    echo "================================================"
    
    # Check if Azure credentials are set
    if [ -z "$AZURE_STORAGE_ACCOUNT_NAME" ] || [ -z "$AZURE_STORAGE_ACCOUNT_KEY" ]; then
        echo -e "${YELLOW}⚠ Azure credentials not set in environment${NC}"
        echo -e "${YELLOW}⚠ Skipping Azure Blob Sink Connector${NC}"
        echo ""
        echo "To enable Azure integration, set these environment variables:"
        echo "  - AZURE_STORAGE_ACCOUNT_NAME"
        echo "  - AZURE_STORAGE_ACCOUNT_KEY"
        echo "  - AZURE_STORAGE_CONTAINER_NAME (optional)"
    else
        # Create Azure Blob sink connector
        create_connector \
            "connectors/sink/connector_azure_ecommerce_all.json" \
            "azure-blob-sink-ecommerce-all"
        
        check_connector_status "azure-blob-sink-ecommerce-all"
    fi
    
    echo ""
    echo "================================================"
    echo "Setup Complete!"
    echo "================================================"
    echo ""
    echo "Available commands:"
    echo ""
    echo "  List all connectors:"
    echo "    curl ${CONNECT_URL}/connectors | jq"
    echo ""
    echo "  Check connector status:"
    echo "    curl ${CONNECT_URL}/connectors/postgres-source-ecommerce-all/status | jq"
    echo ""
    echo "  List Kafka topics:"
    echo "    docker exec broker kafka-topics --bootstrap-server localhost:9092 --list"
    echo ""
    echo "  Consume from topic:"
    echo "    docker exec broker kafka-console-consumer \\"
    echo "      --bootstrap-server localhost:9092 \\"
    echo "      --topic ecommerce.brz_order_items \\"
    echo "      --from-beginning \\"
    echo "      --max-messages 10"
    echo ""
}

# Run main function
main
