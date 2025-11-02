#!/usr/bin/env python3
"""
Kafka Producer for Pollution Data Simulation
Simulates real-time sensor data from different reactor sections
"""

import json
import time
import random
import csv
from datetime import datetime
from kafka import KafkaProducer
import argparse

class PollutionDataProducer:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Define reactor sections
        self.sections = [
            'reactor_zone_A',
            'reactor_zone_B', 
            'reactor_zone_C',
            'control_room',
            'exhaust_stack'
        ]
        
        # Base pollution levels for each section (realistic industrial values)
        self.base_levels = {
            'reactor_zone_A': {'CO': 3.2, 'NOx': 52.0, 'NO2': 38.5, 'temp': 28.5, 'humidity': 65.0},
            'reactor_zone_B': {'CO': 2.8, 'NOx': 48.5, 'NO2': 35.2, 'temp': 27.8, 'humidity': 62.5},
            'reactor_zone_C': {'CO': 3.5, 'NOx': 55.2, 'NO2': 41.0, 'temp': 29.2, 'humidity': 68.0},
            'control_room': {'CO': 1.2, 'NOx': 15.8, 'NO2': 12.5, 'temp': 24.0, 'humidity': 45.0},
            'exhaust_stack': {'CO': 8.5, 'NOx': 125.0, 'NO2': 95.5, 'temp': 45.2, 'humidity': 25.0}
        }

    def generate_sensor_data(self, section):
        """Generate realistic sensor data with some random variation"""
        base = self.base_levels[section]
        
        # Add realistic random variations (±10-20%)
        data = {
            'timestamp': datetime.now().isoformat(),
            'sensor_id': "sensor_{}_{:03d}".format(section, random.randint(1, 3)),
            'section': section,
            'measurements': {
                'CO': round(base['CO'] * (1 + random.uniform(-0.15, 0.15)), 2),
                'NOx': round(base['NOx'] * (1 + random.uniform(-0.12, 0.12)), 2),
                'NO2': round(base['NO2'] * (1 + random.uniform(-0.18, 0.18)), 2),
                'temperature': round(base['temp'] * (1 + random.uniform(-0.08, 0.08)), 1),
                'humidity': round(base['humidity'] * (1 + random.uniform(-0.10, 0.10)), 1)
            },
            'quality_check': random.choice(['good', 'good', 'good', 'warning']),  # 75% good readings
            'batch_id': "batch_{}".format(datetime.now().strftime('%Y%m%d_%H'))
        }
        
        return data

    def simulate_from_csv(self, csv_file, topic='pollution-raw-data', interval=2):
        """Simulate streaming by reading CSV data and adding reactor sections"""
        print("Starting CSV simulation from {}".format(csv_file))
        
        try:
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f, delimiter=';')  # CSV uses semicolon delimiter
                rows = list(reader)
                print("Loaded {} rows from CSV".format(len(rows)))
                
                for index, row in enumerate(rows):
                    # Assign a random section to each CSV row
                    section = random.choice(self.sections)
                    
                    # Helper function to safely convert to float
                    def safe_float(value):
                        if value and value.strip() and value.replace(',', '.').replace('-', '').replace('.', '').isdigit():
                            return float(value.replace(',', '.'))
                        return None
                    
                    # Create message from CSV data
                    message = {
                        'timestamp': datetime.now().isoformat(),
                        'sensor_id': "csv_sensor_{}_{:04d}".format(section, index),
                        'section': section,
                        'measurements': {
                            'CO': safe_float(row.get('CO(GT)', '0')),
                            'NOx': safe_float(row.get('NOx(GT)', '0')),
                            'NO2': safe_float(row.get('NO2(GT)', '0')),
                            'temperature': safe_float(row.get('T', '0')),
                            'humidity': safe_float(row.get('RH', '0'))
                        },
                        'source': 'historical_csv',
                        'csv_row': index
                    }
                    
                    # Send to Kafka
                    self.producer.send(topic, key=section, value=message)
                    print("Sent CSV row {} from section {}".format(index, section))
                    
                    time.sleep(interval)
                    
                    # Break after 50 messages for testing
                    if index >= 50:
                        break
                        
        except Exception as e:
            print("Error reading CSV: {}".format(e))

    def simulate_realtime(self, topic='pollution-raw-data', duration=300, interval=3):
        """Simulate real-time sensor data for specified duration"""
        print("Starting real-time simulation for {} seconds".format(duration))
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration:
            # Generate data for each section
            for section in self.sections:
                data = self.generate_sensor_data(section)
                
                # Send to Kafka
                self.producer.send(topic, key=section, value=data)
                message_count += 1
                print("Sent message {}: {} - CO: {}".format(message_count, section, data['measurements']['CO']))
            
            time.sleep(interval)
        
        print("Simulation complete. Sent {} messages.".format(message_count))

    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()

def main():
    parser = argparse.ArgumentParser(description='Pollution Data Kafka Producer')
    parser.add_argument('--mode', choices=['realtime', 'csv'], default='realtime',
                       help='Simulation mode: realtime or csv')
    parser.add_argument('--csv-file', default='../data/AirQualityUCI.csv',
                       help='Path to CSV file for csv mode')
    parser.add_argument('--duration', type=int, default=120,
                       help='Duration in seconds for realtime mode')
    parser.add_argument('--interval', type=int, default=3,
                       help='Interval between messages in seconds')
    parser.add_argument('--kafka-server', default='localhost:9092',
                       help='Kafka bootstrap server')
    
    args = parser.parse_args()
    
    producer = PollutionDataProducer(args.kafka_server)
    
    try:
        if args.mode == 'csv':
            producer.simulate_from_csv(args.csv_file, interval=args.interval)
        else:
            producer.simulate_realtime(duration=args.duration, interval=args.interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
