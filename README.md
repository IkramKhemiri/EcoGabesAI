# EcoGabes AI - Integrated Air Pollution Analysis System

A comprehensive big data analysis system combining **Apache Spark batch processing** and **Apache Kafka real-time streaming** for studying air pollution patterns in industrial reactor environments.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Docker Container (hadoop-single)              │
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐    │
│  │   Apache Kafka  │  │  Apache Hadoop  │  │   Apache Spark  │    │
│  │  - Zookeeper    │  │    - HDFS       │  │  - Batch Proc.  │    │
│  │  - Topics       │  │    - YARN       │  │  - Streaming    │    │
│  │  - Streaming    │  │                 │  │                 │    │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘    │
│           │                     │                     │            │
│           └─────────────────────┼─────────────────────┘            │
│                                 │                                  │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Data Processing                          │   │
│  │  • Real-time pollution monitoring                          │   │
│  │  • Section-based analytics (Reactor Zones A/B/C)          │   │
│  │  • Historical data analysis                                │   │
│  │  • Alert generation and threshold monitoring               │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
PollutionAnalysis/
├── 📄 README.md                    # This file
├── 📄 .gitignore                   # Git ignore rules
├── 📁 data/
│   └── 📊 AirQualityUCI.csv       # Historical air quality dataset
├── 📁 scripts/
│   ├── 🔧 single_exec.bash        # Main execution script (Spark + HDFS)
│   └── 🐍 pollution_analysis.py   # Enhanced Spark analysis
├── 📁 kafka/
│   ├── 📄 README.md               # Kafka setup documentation
│   ├── 🐍 producer.py             # Real-time data producer
│   ├── 🐍 simple_test.py          # Kafka functionality test
│   └── 🔧 setup_kafka_tp.sh       # Kafka setup (TP method)
├── 📁 config/
│   └── ⚙️ config.yaml             # System configuration
├── 📁 output/                      # Analysis results directory
├── 🔧 setup_single.sh             # Docker container setup
└── 🚀 run_single_demo.sh          # Integrated demo runner
```

## 🌟 Features

### 🔄 **Integrated Data Processing**
- **Batch Analysis**: Historical data processing with Apache Spark
- **Real-time Streaming**: Live pollution monitoring with Kafka
- **Unified Workflow**: Single command execution for complete analysis

### 🏭 **Industrial Reactor Monitoring**
- **Multi-section Analysis**: Reactor zones A, B, C + Control room + Exhaust stack
- **Comprehensive Metrics**: CO, NOx, NO2, Temperature, Humidity
- **Alert System**: Threshold-based pollution warnings

### 📊 **Advanced Analytics**
- **Statistical Analysis**: Descriptive statistics and trends
- **Data Quality**: Sample validation and error handling
- **Result Storage**: HDFS persistence and structured output

## 🚀 Quick Start

### Prerequisites
- Docker installed and running
- 8GB+ RAM recommended
- Linux/macOS environment

### 1. Setup Environment

```bash
# Clone the repository
git clone https://github.com/IkramKhemiri/EcoGabesAI.git
cd EcoGabesAI/PollutionAnalysis

# Set up Docker container
chmod +x setup_single.sh
./setup_single.sh
```

### 2. Run Integrated Analysis

```bash
# Execute complete analysis (Spark + Kafka)
chmod +x run_single_demo.sh
./run_single_demo.sh
```

This will:
- ✅ Start Hadoop services (HDFS, YARN)
- ✅ Launch Kafka and Zookeeper
- ✅ Process historical data with Spark
- ✅ Generate real-time pollution data
- ✅ Perform integrated analytics
- ✅ Save results to HDFS and local output

## 🔧 Manual Usage

### Spark Batch Analysis Only
```bash
# Run only historical data analysis
sudo docker exec hadoop-single bash /root/scripts/single_exec.bash
```

### Kafka Real-time Streaming Only
```bash
# Setup Kafka
sudo docker exec hadoop-single bash /root/kafka/setup_kafka_tp.sh

# Start producer (Terminal 1)
sudo docker exec -it hadoop-single python3 /root/kafka/producer.py --mode realtime --duration 300

# Test consumer (Terminal 2)
sudo docker exec -it hadoop-single python3 /root/kafka/simple_test.py
```

## 📊 Data Flow

### Input Data
- **Historical Dataset**: `AirQualityUCI.csv` (9,000+ records)
- **Real-time Simulation**: Generated sensor data from 5 reactor sections
- **Message Format**: JSON with timestamps, sensor IDs, and measurements

### Processing Pipeline
1. **Data Ingestion**: CSV → HDFS, Sensors → Kafka Topics
2. **Batch Processing**: Spark analytics on historical data
3. **Stream Processing**: Real-time monitoring and alerts
4. **Results Storage**: HDFS files, console output, status reports

### Output Structure
```
output/
├── 📊 statistics/          # Descriptive analytics
├── 📈 sample_data/         # Data validation samples  
├── 📋 status.txt          # Execution status
└── 📄 analysis_results/   # Comprehensive findings
```

## 🎯 Use Cases

### 🏭 **Industrial Monitoring**
- Real-time pollution tracking in reactor facilities
- Multi-zone environmental surveillance
- Regulatory compliance monitoring

### 📈 **Data Analytics**
- Historical trend analysis
- Pollution pattern recognition
- Predictive maintenance insights

### 🚨 **Alert Systems**
- Automatic threshold monitoring
- Emergency response triggers  
- Environmental safety protocols

## 🛠️ Configuration

### System Settings (`config/config.yaml`)
```yaml
hdfs:
  input_path: /user/root/pollution/input/AirQualityUCI.csv
  output_path: /user/root/pollution/output

spark:
  app_name: "Air Pollution Analysis"
  master: "yarn"
```

### Kafka Topics
- `pollution-raw-data`: Real-time sensor measurements
- `pollution-analysis-results`: Processed analytics
- `pollution-alerts`: Critical warnings
- `pollution-data-by-section`: Section-specific data

## 📋 Monitoring & Logs

### Service Status
```bash
# Check running services
sudo docker exec hadoop-single jps

# Expected output:
# - QuorumPeerMain (Zookeeper)
# - Kafka
# - NameNode, DataNode (HDFS)  
# - ResourceManager, NodeManager (YARN)
```

### Web Interfaces
- **HDFS**: http://localhost:9870
- **YARN**: http://localhost:8088  
- **Container**: `sudo docker logs hadoop-single`

## 🔍 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   sudo docker stop hadoop-single
   sudo docker start hadoop-single
   ```

2. **Kafka Connection Issues**
   ```bash
   # Restart Kafka services
   sudo docker exec hadoop-single /root/kafka/setup_kafka_tp.sh
   ```

3. **HDFS Storage Issues**
   ```bash
   # Check HDFS status
   sudo docker exec hadoop-single hdfs dfsadmin -report
   ```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-analysis`)
3. Commit changes (`git commit -am 'Add new analysis'`)
4. Push to branch (`git push origin feature/new-analysis`)
5. Create Pull Request

## 📄 License

This project is part of the EcoGabes AI initiative for environmental monitoring and analysis.

## 📞 Support

For issues and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review component-specific READMEs in subdirectories

---
**EcoGabes AI** - Advanced Environmental Data Analytics Platform
- NO2 (Nitrogen Dioxide)
- NOx (Nitrogen Oxides)
- Other air quality indicators

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  