#!/bin/bash

# Function to check if command succeeded
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
}

echo "Setting up Hadoop cluster..."

# Create custom network with DNS resolution
if ! sudo docker network ls | grep -q hadoop-network; then
    echo "Creating Docker network with DNS..."
    sudo docker network create --driver bridge \
        --subnet=172.20.0.0/16 \
        --ip-range=172.20.240.0/20 \
        hadoop-network
    check_error "Failed to create network"
else
    echo "hadoop-network already exists"
fi

# Pull the image
echo "Pulling Hadoop image..."
sudo docker pull liliasfaxi/spark-hadoop:hv-2.7.2
check_error "Failed to pull image"

# Function to create container
create_container() {
    local name=$1
    local hostname=$1
    local ports=$2
    
    # Remove existing container if it exists
    if sudo docker ps -a | grep -q $name; then
        echo "Removing existing $name container..."
        sudo docker rm -f $name
    fi
    
    echo "Creating $name container..."
    sudo docker run -d --name $name --hostname $hostname \
        $ports \
        --network hadoop-network \
        --dns 8.8.8.8 \
        --restart unless-stopped \
        liliasfaxi/spark-hadoop:hv-2.7.2 \
        /bin/bash -c "service ssh start && tail -f /dev/null"
    check_error "Failed to create $name"
}

# Create master
create_container "hadoop-master" "-p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010"

# Create slaves
create_container "hadoop-slave1" "-p 8040:8042"
create_container "hadoop-slave2" "-p 8041:8042"

echo "Hadoop cluster setup complete!"
echo "You can now run ./run_demo.sh"
