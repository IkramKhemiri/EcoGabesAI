#!/bin/bash

# Single-node demo script
echo "=== Single-Node Pollution Analysis Demo ==="

# Function to check if command succeeded
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
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

# Copy files
copy_files() {
    echo "Copying project files..."
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/data/AirQualityUCI.csv hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/scripts hadoop-single:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/config hadoop-single:/root/
}

# Run analysis
run_analysis() {
    echo "Running single-node analysis..."
    sudo docker exec hadoop-single bash -c "cd /root/scripts && ./single_exec.bash"
}

# Copy results back
copy_results() {
    echo "Copying results back to host..."
    mkdir -p /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output
    sudo docker cp hadoop-single:/root/output/. /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output/
    echo "Results available in: /home/ken2/ii3/BigData/TP3/PollutionAnalysis/output/"
}

# Main execution
start_container
copy_files
run_analysis
copy_results

echo "=== Demo Complete! ==="
echo "Check the output directory for results"
