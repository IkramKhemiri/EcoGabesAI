#!/bin/bash

# Function to check if command succeeded
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
}

# Start containers if not running
docker_start() {
    for container in hadoop-master hadoop-slave1 hadoop-slave2; do
        if [ "$(sudo docker ps -q -f name=$container)" ]; then
            echo "$container is running"
        else
            echo "Starting $container..."
            sudo docker start $container
            check_error "Failed to start $container"
            echo "Waiting for $container to be ready..."
            sleep 5
            
            # Verify container is still running after start
            if [ -z "$(sudo docker ps -q -f name=$container)" ]; then
                echo "Error: $container exited after start. Recreating..."
                sudo docker rm -f $container
                echo "Please run ./setup_cluster.sh first to recreate containers properly"
                exit 1
            fi
        fi
    done
}

# Copy project files
copy_files() {
    echo "Copying project files..."
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/data/AirQualityUCI.csv hadoop-master:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/scripts hadoop-master:/root/
    sudo docker cp /home/ken2/ii3/BigData/TP3/PollutionAnalysis/config hadoop-master:/root/
    sudo docker exec hadoop-master chmod +x /root/scripts/exec.bash
}

# Run analysis
run_analysis() {
    echo "Running analysis..."
    
    # Try full analysis first
    if sudo docker exec hadoop-master bash -c "cd /root/scripts && ./exec.bash"; then
        echo "Full analysis completed successfully"
    else
        echo "Full analysis failed, trying simplified version..."
        sudo docker exec hadoop-master bash -c "cd /root/scripts && ./simple_exec.bash"
    fi
}

# Main execution
echo "Starting Pollution Analysis Demo..."
docker_start
copy_files
run_analysis
