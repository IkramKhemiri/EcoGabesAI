#!/bin/bash

echo "=== Starting Kafka and Zookeeper (TP Method) ==="

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Start Zookeeper first
start_zookeeper() {
    log "Starting Zookeeper..."
    sudo docker exec -d hadoop-single bash -c "
        cd /usr/local/kafka
        ./bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
    "
    sleep 5
}

# Start Kafka
start_kafka() {
    log "Starting Kafka..."
    sudo docker exec -d hadoop-single bash -c "
        cd /usr/local/kafka
        ./bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
    "
    sleep 10
}

# Verify services are running
verify_services() {
    log "Verifying Java processes..."
    sudo docker exec hadoop-single jps
}

# Create topic as in TP
create_hello_topic() {
    log "Creating Hello-Kafka topic..."
    sudo docker exec hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Hello-Kafka
    "
}

# List topics
list_topics() {
    log "Listing topics..."
    sudo docker exec hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
    "
}

# Test producer (send a test message)
test_producer() {
    log "Testing producer..."
    echo "Test message from setup" | sudo docker exec -i hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka
    "
}

# Test consumer (read messages)
test_consumer() {
    log "Testing consumer (will timeout after 5 seconds)..."
    timeout 5s sudo docker exec hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Hello-Kafka --from-beginning
    " || log "Consumer test completed"
}

# Main execution following TP steps
main() {
    start_zookeeper
    start_kafka
    verify_services
    create_hello_topic
    list_topics
    test_producer
    test_consumer
    
    log "=== Kafka Setup Complete (TP Method) ==="
    log "You can now:"
    log "1. Create topics: docker exec hadoop-single /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic YOUR_TOPIC"
    log "2. Start producer: docker exec -it hadoop-single /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic YOUR_TOPIC"  
    log "3. Start consumer: docker exec -it hadoop-single /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic YOUR_TOPIC --from-beginning"
}

# Run the setup
main
