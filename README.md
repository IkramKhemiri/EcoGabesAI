# Air Quality Analysis with Hadoop and Spark

This project analyzes air quality data using a single-node Hadoop cluster with Spark, all containerized using Docker.

## Architecture

```
┌─────────────────────────────────────┐
│           Docker Container          │
│  ┌────────────────┐    ┌────────┐  │
│  │ Apache Hadoop  │    │ Apache │  │
│  │  - HDFS       │<-->│ Spark  │  │
│  │  - YARN       │    │        │  │
│  └────────────────┘    └────────┘  │
│            ▲                ▲       │
│            │                │       │
│            ▼                ▼       │
│  ┌─────────────────────────────┐   │
│  │        Input Data           │   │
│  │    AirQualityUCI.csv       │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

## Project Structure

```
pollution-analysis/
├── data/
│   └── AirQualityUCI.csv       # Input data file
├── scripts/
│   ├── single_exec.bash        # Main execution script
│   ├── pollution_analysis.py   # Spark analysis script
│   └── visualization.py        # Data visualization script
├── config/
│   └── config.yaml            # Configuration files
├── setup_single.sh            # Container setup script
└── run_single_demo.sh         # Demo runner script
```


1. Clone this repository:
```bash
git clone <repository-url>
cd pollution-analysis
```

2. Set up the Hadoop container:
```bash
chmod +x setup_single.sh
./setup_single.sh
```

## Running the Analysis

1. Start the analysis:
```bash
chmod +x run_single_demo.sh
./run_single_demo.sh
```

2. Check the results in the `output/` directory:
- `data_summary.txt`: Basic statistics about the dataset
- `pollution_analysis_report.txt`: Detailed analysis report
- `data_preview.txt`: Sample of the analyzed data

## Technical Details

### Components

- **Hadoop 2.7.2**: Distributed storage and processing
  - HDFS for data storage
  - YARN for resource management
- **Apache Spark**: Data processing engine
- **Docker**: Containerization
- **Python**: Analysis scripts

### Data Flow

1. Data is loaded into HDFS
2. Spark reads data from HDFS
3. Analysis is performed using PySpark
4. Results are saved back to HDFS
5. Reports are generated and copied to host machine

### Container Details

- Base Image: `liliasfaxi/spark-hadoop:hv-2.7.2`
- Exposed Ports:
  - 9870: Hadoop NameNode web UI
  - 8088: YARN ResourceManager web UI
  - 7077: Spark Master web UI
  - 16010: HBase Master web UI



## Web Interfaces

- Hadoop NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- Spark Master: http://localhost:7077

## Dataset Information

The Air Quality dataset (`AirQualityUCI.csv`) contains hourly measurements of:
- CO (Carbon Monoxide)
- NO2 (Nitrogen Dioxide)
- NOx (Nitrogen Oxides)
- Other air quality indicators

