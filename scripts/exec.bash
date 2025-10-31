#!/bin/bash

set -e

log() { echo "[exec] $1"; }

# Function to check if service is running
check_service() {
    jps | grep -q "$1"
}

setup_env() {
    log "Installing Python dependencies..."
    
    # Skip package installation if already done
    if [ -f "/tmp/packages_installed" ]; then
        log "Packages already installed, skipping..."
        return 0
    fi
    
    # Update package lists
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y python3-pip python3-setuptools python3-dev build-essential wget curl >/dev/null 2>&1 || true
    
    # Try to install packages, but continue if it fails (offline mode)
    log "Attempting to install Python packages..."
    pip3 install --no-cache-dir --upgrade 'pip<21.0' 2>/dev/null || log "Pip upgrade failed, continuing..."
    pip3 install --no-cache-dir 'matplotlib<3.1' 2>/dev/null || log "matplotlib install failed, continuing..."
    pip3 install --no-cache-dir 'pandas<1.0' 2>/dev/null || log "pandas install failed, continuing..."
    pip3 install --no-cache-dir 'seaborn<0.10' 2>/dev/null || log "seaborn install failed, continuing..."
    pip3 install --no-cache-dir happybase 2>/dev/null || log "happybase install failed, continuing..."
    pip3 install --no-cache-dir pyyaml 2>/dev/null || log "pyyaml install failed, continuing..."
    
    # Mark as done
    touch /tmp/packages_installed
    log "Package installation completed (some may have failed but continuing)"
}

start_hadoop() {
    log "Starting Hadoop..."
    if ! check_service NameNode; then
        cd /root
        ./start-hadoop.sh || true
        sleep 10
    fi
    # Wait for HDFS
    for i in {1..10}; do
        if hdfs dfs -ls / >/dev/null 2>&1; then
            log "HDFS is ready"; break
        fi
        log "Waiting for HDFS (attempt $i)..."; sleep 5
    done
}

start_hbase() {
    log "Starting HBase..."
    if ! check_service HMaster; then
        start-hbase.sh || true
        sleep 8
    fi
}

prepare_hdfs() {
    log "Preparing HDFS paths..."
    hdfs dfs -mkdir -p /user/root || true
    hdfs dfs -mkdir -p /user/root/pollution/input || true
    hdfs dfs -mkdir -p /user/root/pollution/output || true
    hdfs dfs -put -f /root/AirQualityUCI.csv /user/root/pollution/input/
}

run_spark_job() {
    log "Running Spark job..."
    export PYTHONPATH="/root/scripts:$PYTHONPATH"
    spark-submit \
        --master yarn \
        --conf spark.pyspark.python=/usr/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/bin/python3 \
        --py-files /root/scripts/hbase_storage.py \
        /root/scripts/main.py
}

fetch_and_visualize() {
    log "Fetching results and generating visuals..."
    mkdir -p /root/output
    hdfs dfs -get -f /user/root/pollution/output/daily_metrics/part-* /root/output/ || true
    python3 /root/scripts/visualization.py || true
}

log "Starting Pollution Analysis pipeline"
setup_env
start_hadoop
start_hbase
prepare_hdfs
run_spark_job
fetch_and_visualize
log "Done. Outputs in /root/output"