#!/bin/bash
# Verification script for E-commerce Event Generator setup

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "================================================"
echo "E-commerce Analytics - Setup Verification"
echo "================================================"
echo ""

# Function to check if a service is running
check_service() {
    service_name=$1
    container_name=$2
    
    if docker ps | grep -q "$container_name"; then
        echo -e "${GREEN}✓${NC} $service_name is running"
        return 0
    else
        echo -e "${RED}✗${NC} $service_name is NOT running"
        return 1
    fi
}

# Function to check PostgreSQL tables
check_postgres_tables() {
    echo ""
    echo -e "${BLUE}Checking PostgreSQL tables...${NC}"
    
    tables=("brz_category" "brz_brands" "brz_customers" "brz_calendar" "brz_products" "brz_order_items")
    
    for table in "${tables[@]}"; do
        count=$(docker exec postgres psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
        
        if [ ! -z "$count" ] && [ "$count" -gt 0 ]; then
            echo -e "${GREEN}✓${NC} $table: $count rows"
        else
            echo -e "${RED}✗${NC} $table: No data or table doesn't exist"
        fi
    done
}

# Function to check Kafka topics
check_kafka_topics() {
    echo ""
    echo -e "${BLUE}Checking Kafka topics...${NC}"
    
    topics=$(docker exec broker kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep "ecommerce" || echo "")
    
    if [ -z "$topics" ]; then
        echo -e "${YELLOW}⚠${NC} No e-commerce topics found"
        echo "   This is normal if connectors haven't been set up yet"
    else
        echo -e "${GREEN}✓${NC} E-commerce topics found:"
        echo "$topics" | while read topic; do
            echo "   - $topic"
        done
    fi
}

# Function to check Kafka Connect
check_kafka_connect() {
    echo ""
    echo -e "${BLUE}Checking Kafka Connect...${NC}"
    
    if curl -sf http://localhost:8083 > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} Kafka Connect is accessible"
        
        connectors=$(curl -s http://localhost:8083/connectors 2>/dev/null || echo "[]")
        connector_count=$(echo "$connectors" | jq '. | length' 2>/dev/null || echo "0")
        
        echo "   Active connectors: $connector_count"
        
        if [ "$connector_count" -gt 0 ]; then
            echo "$connectors" | jq -r '.[]' 2>/dev/null | while read connector; do
                status=$(curl -s "http://localhost:8083/connectors/$connector/status" | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
                if [ "$status" = "RUNNING" ]; then
                    echo -e "   ${GREEN}✓${NC} $connector: $status"
                else
                    echo -e "   ${RED}✗${NC} $connector: $status"
                fi
            done
        fi
    else
        echo -e "${RED}✗${NC} Kafka Connect is not accessible"
    fi
}

# Function to check event generator
check_event_generator() {
    echo ""
    echo -e "${BLUE}Checking Event Generator...${NC}"
    
    if docker ps | grep -q "event-generator"; then
        echo -e "${GREEN}✓${NC} Event Generator is running"
        
        # Check recent logs for errors
        errors=$(docker logs --tail 50 event-generator 2>&1 | grep -i error || echo "")
        
        if [ -z "$errors" ]; then
            echo -e "${GREEN}✓${NC} No recent errors in logs"
        else
            echo -e "${YELLOW}⚠${NC} Recent errors found in logs:"
            echo "$errors" | head -3
        fi
        
        # Check if data is being generated
        last_log=$(docker logs --tail 10 event-generator 2>&1 | grep "Wrote" | tail -1 || echo "")
        if [ ! -z "$last_log" ]; then
            echo -e "${GREEN}✓${NC} Data generation active"
            echo "   Last: $last_log"
        fi
    else
        echo -e "${RED}✗${NC} Event Generator is NOT running"
    fi
}

# Main verification
echo -e "${BLUE}Step 1: Checking Docker Services${NC}"
echo ""

check_service "Zookeeper" "zookeeper"
check_service "Kafka Broker" "broker"
check_service "Schema Registry" "schema-registry"
check_service "Kafka Connect" "connect"
check_service "PostgreSQL" "postgres"
check_service "Event Generator" "event-generator"

echo ""
echo -e "${BLUE}Step 2: Checking Data${NC}"
check_postgres_tables

echo ""
echo -e "${BLUE}Step 3: Checking Kafka Infrastructure${NC}"
check_kafka_topics
check_kafka_connect

echo ""
echo -e "${BLUE}Step 4: Checking Event Generator${NC}"
check_event_generator

echo ""
echo "================================================"
echo "Verification Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo ""
echo "1. If Event Generator is not running:"
echo "   docker-compose up -d event-generator"
echo ""
echo "2. To set up Kafka connectors:"
echo "   bash scripts/setup_connectors.sh"
echo ""
echo "3. To view Event Generator logs:"
echo "   docker logs -f event-generator"
echo ""
echo "4. To check PostgreSQL data:"
echo "   docker exec -it postgres psql -U postgres -d postgres"
echo ""
echo "5. To consume Kafka messages:"
echo "   docker exec broker kafka-console-consumer \\"
echo "     --bootstrap-server localhost:9092 \\"
echo "     --topic ecommerce.brz_order_items \\"
echo "     --from-beginning --max-messages 10"
echo ""
