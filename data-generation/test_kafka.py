# data-generation/test_kafka_connection.py
from kafka import KafkaProducer, KafkaConsumer
import json

print("🧪 Test de connexion Kafka avec Python...")

try:
    # Test producteur
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Message test
    test_data = {
        "sensor_id": "GABES_TEST",
        "timestamp": "2024-01-01T12:00:00",
        "so2": 45.5,
        "pm2_5": 23.1,
        "status": "TEST_CONNECTION"
    }
    
    producer.send('pollution-data', test_data)
    producer.flush()
    print("✅ Message envoyé à Kafka!")
    
    # Test consommateur
    consumer = KafkaConsumer(
        'pollution-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000
    )
    
    print("📨 Messages dans le topic:")
    message_count = 0
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print(f"   📍 {data['sensor_id']} - SO₂: {data['so2']}")
        message_count += 1
    
    if message_count == 0:
        print("   ℹ️ Aucun message trouvé (c'est normal au début)")
    
    consumer.close()
    producer.close()
    print("🎉 Connexion Python-Kafka fonctionne!")
    
except Exception as e:
    print(f"❌ Erreur: {e}")