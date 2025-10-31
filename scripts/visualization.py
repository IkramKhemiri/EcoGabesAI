import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import glob
import os

def read_data():
    # Look for CSV files in /root/output directory
    csv_files = glob.glob('/root/output/part-*.csv')
    if not csv_files:
        print("No CSV files found in /root/output/")
        return None
    
    # Read and combine all part files
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None

def create_daily_trend_plot(data):
    plt.figure(figsize=(15, 8))
    
    # Check which columns exist
    if 'avg_co' in data.columns:
        plt.plot(range(len(data)), data['avg_co'], label='CO', marker='o')
    if 'avg_nox' in data.columns:
        plt.plot(range(len(data)), data['avg_nox'], label='NOx', marker='s')
    if 'avg_no2' in data.columns:
        plt.plot(range(len(data)), data['avg_no2'], label='NO2', marker='^')
    
    plt.title('Daily Average Pollution Levels')
    plt.xlabel('Time Period')
    plt.ylabel('Concentration (µg/m³)')
    plt.legend()
    plt.tight_layout()
    
    os.makedirs('/root/output', exist_ok=True)
    plt.savefig('/root/output/daily_trends.png')
    plt.close()
    print("Created daily_trends.png")

def create_correlation_heatmap(data):
    numeric_cols = [col for col in ['avg_co', 'avg_nox', 'avg_no2'] if col in data.columns]
    
    if len(numeric_cols) < 2:
        print("Not enough numeric columns for correlation")
        return
    
    corr_matrix = data[numeric_cols].corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Pollutant Correlation Heatmap')
    plt.tight_layout()
    
    plt.savefig('/root/output/correlation_heatmap.png')
    plt.close()
    print("Created correlation_heatmap.png")

def main():
    data = read_data()
    if data is not None and len(data) > 0:
        print(f"Loaded {len(data)} rows of data")
        print("Columns:", list(data.columns))
        create_daily_trend_plot(data)
        create_correlation_heatmap(data)
    else:
        print("No data available for visualization")

if __name__ == "__main__":
    main()