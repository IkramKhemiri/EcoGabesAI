# Kafka Integration Module

Real-time streaming component for the EcoGabes AI pollution analysis system.

## � Quick Usage

This module is automatically integrated with the main system. Use the parent directory's `run_single_demo.sh` for complete integration.

### Manual Kafka Operations

#### Setup Kafka (TP Method)
```bash
sudo docker exec hadoop-single bash /root/kafka/setup_kafka_tp.sh
```

#### Test Producer
```bash
sudo docker exec -it hadoop-single python3 /root/kafka/producer.py --mode realtime --duration 120
```

#### Test Basic Functionality
```bash
sudo docker exec -it hadoop-single python3 /root/kafka/simple_test.py
```

## � Components

- **`producer.py`**: Real-time data generator for 5 reactor sections
- **`simple_test.py`**: Basic Kafka producer/consumer test
- **`setup_kafka_tp.sh`**: Kafka setup following TP methodology

## 🎯 Features

- **Multi-section simulation**: Reactor zones A/B/C, control room, exhaust stack
- **Realistic data patterns**: CO, NOx, NO2, temperature, humidity with variations
- **JSON message format**: Structured data with timestamps and metadata
- **Topic organization**: Section-based data partitioning

## 📋 Kafka Topics

| Topic | Purpose | Partitions |
|-------|---------|------------|
| `pollution-raw-data` | Real-time sensor data | 3 |
| `pollution-analysis-results` | Processed analytics | 3 |
| `pollution-alerts` | Critical warnings | 1 |
| `pollution-data-by-section` | Section-specific data | 5 |

## 🔧 Configuration

Data generation parameters can be adjusted in `producer.py`:
- Base pollution levels per section
- Variation ranges (±10-20%)
- Message intervals and batch sizes
- Sensor ID patterns

For complete system integration, see the main README in the parent directory.
