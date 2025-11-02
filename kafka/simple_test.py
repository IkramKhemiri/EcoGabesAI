#!/usr/bin/env python3
"""
Simple Kafka Test for Pollution Data
Tests producer and consumer functionality
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

def test_producer():
    """Test Kafka producer"""
    print("=== Testing Kafka Producer ===")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Simulate pollution data from different sections
    sections = ['reactor_zone_A', 'reactor_zone_B', 'reactor_zone_C', 'control_room', 'exhaust_stack']
    
    for i in range(10):
        for section in sections:
            data = {
                'timestamp': datetime.now().isoformat(),
                'sensor_id': 'sensor_{}_{:03d}'.format(section, i),
                'section': section,
                'measurements': {
                    'CO': round(2.5 + (i * 0.1), 2),
                    'NOx': round(45.0 + (i * 2.0), 2),
                    'NO2': round(32.0 + (i * 1.5), 2),
                    'temperature': round(25.0 + (i * 0.5), 1),
                    'humidity': round(65.0 + (i * 0.8), 1)
                },
                'batch_id': 'test_batch_{}'.format(i)
            }
            
            producer.send('pollution-raw-data', value=data)
            print("Sent: {} - CO: {}".format(section, data['measurements']['CO']))
        
        time.sleep(1)
    
    producer.flush()
    producer.close()
    print("Producer test completed!")

def test_consumer():
    """Test Kafka consumer"""
    print("\n=== Testing Kafka Consumer ===")
    
    consumer = KafkaConsumer(
        'pollution-raw-data',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # 10 second timeout
    )
    
    message_count = 0
    for message in consumer:
        data = message.value
        print("Received: {} - CO: {}".format(data['section'], data['measurements']['CO']))
        message_count += 1
        
        if message_count >= 20:  # Read first 20 messages
            break
    
    consumer.close()
    print("Consumer test completed! Read {} messages.".format(message_count))

def main():
    print("=== Kafka Integration Test ===")
    test_producer()
    test_consumer()
    print("\n=== Test Complete! ===")

if __name__ == "__main__":
    main()
