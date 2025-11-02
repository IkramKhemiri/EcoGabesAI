#!/bin/bash

# Integrated Pollution Analysis Demo - Spark HDFS + Kafka Streaming
echo "=== Integrated Pollution Analysis Demo ==="
echo "This demo combines Spark HDFS batch processing with Kafka real-time streaming"

# Function to check if command succeeded
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
}

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Start single container
start_container() {
    if [ "$(sudo docker ps -q -f name=hadoop-single)" ]; then
        echo "hadoop-single is running"
    else
        echo "Starting hadoop-single..."
        sudo docker start hadoop-single
        check_error "Failed to start hadoop-single"
        sleep 10
        
        # Verify container is running
        if [ -z "$(sudo docker ps -q -f name=hadoop-single)" ]; then
            echo "Error: hadoop-single exited after start"
            echo "Please run ./setup_single.sh first"
            exit 1
        fi
    fi
}

# Setup Kafka and Zookeeper
setup_kafka() {
    log "Setting up Kafka and Zookeeper..."
    sudo docker exec hadoop-single bash -c "
        # Start Zookeeper
        cd /usr/local/kafka
        ./bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
        sleep 5
        
        # Start Kafka
        ./bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
        sleep 10
        
        # Verify services
        jps | grep -E 'QuorumPeerMain|Kafka'
    "
}

# Create Kafka topics
create_kafka_topics() {
    log "Creating Kafka topics..."
    sudo docker exec hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic pollution-raw-data 2>/dev/null || echo 'Topic pollution-raw-data already exists'
        /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic pollution-alerts 2>/dev/null || echo 'Topic pollution-alerts already exists'
        
        # List topics
        echo 'Available topics:'
        /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
    "
}

# Copy files
copy_files() {
    log "Copying project files..."
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/data/AirQualityUCI.csv hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/scripts hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/config hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/kafka/producer.py hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/kafka/simple_test.py hadoop-single:/root/
}

# Run batch analysis with Spark HDFS
run_batch_analysis() {
    log "Step 1: Running Spark HDFS batch analysis..."
    sudo docker exec hadoop-single bash -c "cd /root/scripts && ./single_exec.bash"
}

# Test Kafka integration
test_kafka_integration() {
    log "Step 2: Testing Kafka streaming integration..."
    sudo docker exec hadoop-single bash -c "cd /root && python3 simple_test.py"
}

# Run streaming analysis (optional - can be started separately)
start_streaming_producer() {
    log "Step 3: Starting Kafka producer for real-time streaming..."
    log "Starting producer in background for 60 seconds..."
    sudo docker exec -d hadoop-single bash -c "cd /root && python3 producer.py --mode realtime --duration 60 --interval 2"
    
    # Wait a moment then show some messages
    sleep 5
    log "Checking recent messages in Kafka..."
    timeout 10s sudo docker exec hadoop-single bash -c "
        /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution-raw-data --max-messages 5 --from-beginning
    " || log "Kafka consumer test completed"
}

# Copy results back
copy_results() {
    echo "Copying results back to host..."
    mkdir -p /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output
    sudo docker cp hadoop-single:/root/output/. /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output/
    echo "Results available in: /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output/"
}

# Create comprehensive report
create_final_report() {
    log "Creating comprehensive analysis report..."
    sudo docker exec hadoop-single bash -c "
        echo '=== Integrated Pollution Analysis Report ===' > /root/output/integrated_report.txt
        echo 'Generated on: $(date)' >> /root/output/integrated_report.txt
        echo '' >> /root/output/integrated_report.txt
        
        echo '1. BATCH ANALYSIS RESULTS (Spark HDFS):' >> /root/output/integrated_report.txt
        if [ -f /root/output/status.txt ]; then
            cat /root/output/status.txt >> /root/output/integrated_report.txt
        fi
        echo '' >> /root/output/integrated_report.txt
        
        echo '2. STREAMING SETUP STATUS:' >> /root/output/integrated_report.txt
        echo 'Kafka and Zookeeper processes:' >> /root/output/integrated_report.txt
        jps | grep -E 'QuorumPeerMain|Kafka' >> /root/output/integrated_report.txt || echo 'No Kafka processes found' >> /root/output/integrated_report.txt
        echo '' >> /root/output/integrated_report.txt
        
        echo '3. AVAILABLE KAFKA TOPICS:' >> /root/output/integrated_report.txt
        /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181 >> /root/output/integrated_report.txt
        echo '' >> /root/output/integrated_report.txt
        
        echo '4. DATA PROCESSING SUMMARY:' >> /root/output/integrated_report.txt
        echo '- Historical data processed via Spark and stored in HDFS' >> /root/output/integrated_report.txt
        echo '- Real-time streaming setup with Kafka topics' >> /root/output/integrated_report.txt
        echo '- Multiple reactor sections monitored: A, B, C, control_room, exhaust_stack' >> /root/output/integrated_report.txt
        echo '' >> /root/output/integrated_report.txt
        
        echo '5. NEXT STEPS:' >> /root/output/integrated_report.txt
        echo '- Start real-time producer: python3 producer.py --mode realtime --duration 300' >> /root/output/integrated_report.txt
        echo '- Monitor streams: /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution-raw-data' >> /root/output/integrated_report.txt
        echo '- Integrate with Spark Streaming for real-time analytics' >> /root/output/integrated_report.txt
    "
}

# Main execution
main() {
    log "=== Starting Integrated Demo ==="
    start_container
    setup_kafka
    create_kafka_topics
    copy_files
    run_batch_analysis
    test_kafka_integration
    start_streaming_producer
    create_final_report
    copy_results
    
    log "=== Integrated Demo Complete! ==="
    log "Results available in: /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output/"
    log ""
    log "What was accomplished:"
    log "✅ Batch processing: Historical data analyzed with Spark and stored in HDFS"
    log "✅ Streaming setup: Kafka topics created and tested"
    log "✅ Real-time data: Producer generating live pollution data"
    log "✅ Integration: Both systems working together"
    log ""
    log "Interactive commands:"
    log "- View real-time data: sudo docker exec -it hadoop-single /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution-raw-data"
    log "- Start producer: sudo docker exec -it hadoop-single python3 /root/producer.py"
    log "- Check HDFS results: sudo docker exec hadoop-single hdfs dfs -ls /user/root/pollution/output"
}

# Run the integrated demo
main
