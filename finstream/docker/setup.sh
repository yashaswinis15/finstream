#!/bin/bash
set -e
set -o pipefail

BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

MAX_RETRIES=20

DB_USER=${POSTGRES_USER:-finstream}
DB_NAME=${POSTGRES_DB:-fintechdb}

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  FinStream — Setup & Verification${NC}"
echo -e "${BLUE}================================================${NC}"

# ----------------------------
# Check container exists
# ----------------------------
check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "$1"; then
        echo -e "${RED}  Container $1 is not running.${NC}"
        exit 1
    fi
}

# ----------------------------
# Wait for Postgres
# ----------------------------
echo -e "\n${BLUE}[1/4] Waiting for PostgreSQL...${NC}"
check_container finstream_postgres

i=0
until docker exec finstream_postgres pg_isready -U "$DB_USER" -d "$DB_NAME" > /dev/null 2>&1; do
    if [ $i -ge $MAX_RETRIES ]; then
        echo -e "${RED}  Postgres failed to start.${NC}"
        exit 1
    fi
    sleep 3
    ((i++))
done
echo -e "${GREEN}  Postgres ready.${NC}"

# ----------------------------
# Wait for Kafka (port check)
# ----------------------------
echo -e "\n${BLUE}[2/4] Waiting for Kafka...${NC}"
check_container finstream_kafka

i=0
until nc -z localhost 9092; do
    if [ $i -ge $MAX_RETRIES ]; then
        echo -e "${RED}  Kafka not reachable.${NC}"
        exit 1
    fi
    sleep 5
    ((i++))
done
echo -e "${GREEN}  Kafka reachable.${NC}"

# ----------------------------
# Wait for Debezium
# ----------------------------
echo -e "\n${BLUE}[3/4] Waiting for Debezium Connect...${NC}"

i=0
until curl -sf http://localhost:8083/ > /dev/null 2>&1; do
    if [ $i -ge $MAX_RETRIES ]; then
        echo -e "${RED}  Debezium failed to start.${NC}"
        exit 1
    fi
    sleep 5
    ((i++))
done
echo -e "${GREEN}  Debezium ready.${NC}"

# ----------------------------
# Register connector
# ----------------------------
echo -e "\n${BLUE}[4/4] Registering Debezium CDC connector...${NC}"

if curl -s http://localhost:8083/connectors | grep -q "finstream-postgres-cdc"; then
    echo "  Re-registering existing connector..."
    curl -sf -X DELETE http://localhost:8083/connectors/finstream-postgres-cdc > /dev/null
    sleep 3
fi

RESULT=$(curl -s -o /tmp/debezium_response.json -w "%{http_code}" \
    -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @../kafka/debezium-connector.json)

if [ "$RESULT" = "201" ] || [ "$RESULT" = "200" ]; then
    echo -e "${GREEN}  Connector registered successfully.${NC}"
else
    echo -e "${RED}  Connector registration failed (HTTP $RESULT).${NC}"
    cat /tmp/debezium_response.json
    exit 1
fi

sleep 5

# ----------------------------
# Verify connector
# ----------------------------
STATUS=$(curl -s http://localhost:8083/connectors/finstream-postgres-cdc/status | grep -o '"state":"[^"]*"' | cut -d':' -f2 | tr -d '"')

if [ "$STATUS" = "RUNNING" ]; then
    echo -e "${GREEN}  Connector state: RUNNING${NC}"
else
    echo -e "${RED}  Connector state: $STATUS${NC}"
fi

# ----------------------------
# Summary
# ----------------------------
echo -e "\n${GREEN}================================================${NC}"
echo -e "${GREEN}  FinStream is ready!${NC}"
echo -e "${GREEN}================================================${NC}"

echo ""
echo -e "  Kafka UI   →  ${BLUE}http://localhost:8080${NC}"
echo -e "  Grafana    →  ${BLUE}http://localhost:3000${NC}"
echo -e "  Debezium   →  ${BLUE}http://localhost:8083${NC}"
echo -e "  MinIO      →  ${BLUE}http://localhost:9001${NC}"
echo ""
echo -e "  Next steps:"
echo -e "    1. Run simulator"
echo -e "    2. Check Kafka topics"
echo -e "    3. Start Spark streaming"
echo -e "    4. Start alert engine"