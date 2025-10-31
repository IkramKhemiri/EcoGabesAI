#!/bin/bash

# Single node Hadoop setup script
echo "Setting up single-node Hadoop cluster..."

# Function to check if command succeeded
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
}

# Remove existing container
if sudo docker ps -a | grep -q hadoop-single; then
    echo "Removing existing hadoop-single container..."
    sudo docker rm -f hadoop-single
fi

# Pull the image
echo "Pulling Hadoop image..."
sudo docker pull liliasfaxi/spark-hadoop:hv-2.7.2
check_error "Failed to pull image"

# Create single container with all services
echo "Creating hadoop-single container..."
sudo docker run -d --name hadoop-single --hostname hadoop-single \
    -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
    --dns 8.8.8.8 \
    --restart unless-stopped \
    liliasfaxi/spark-hadoop:hv-2.7.2 \
    /bin/bash -c "service ssh start && tail -f /dev/null"
check_error "Failed to create hadoop-single"

echo "Single-node Hadoop setup complete!"
echo "You can now run the single-node demo"
