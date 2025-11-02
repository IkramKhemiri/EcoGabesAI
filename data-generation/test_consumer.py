from kafka import KafkaConsumer
import json

print("👂 EcoGabes - Consumer de test")
print("⏳ Connexion à Kafka...")

try:
    consumer = KafkaConsumer(
        'pollution-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='ecoGabesTestGroup',  # Groupe fixe pour lire depuis le début
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000
    )
    
    print("✅ Connecté à Kafka! Messages reçus:")
    print("-" * 100)
    
    message_count = 0
    for message in consumer:
        data = message.value
        risk_icon = "🟢" if data["health_risk"] == "LOW" else "🟡" if data["health_risk"] == "MODERATE" else "🟠" if data["health_risk"] == "MEDIUM" else "🔴"
        
        print(f"{risk_icon} [{data['sensor_id']:12}] "
              f"SO₂: {data['metrics']['so2']:5.1f} | "
              f"NO₂: {data['metrics']['no2']:5.1f} | "
              f"PM2.5: {data['metrics']['pm2_5']:5.1f} | "
              f"Temp: {data['metrics']['temperature']:4.1f}°C | "
              f"AQI: {data['aqi']:5.1f} | "
              f"{data['health_risk']}")
        
        message_count += 1
    
    if message_count == 0:
        print("⏳ Aucun message reçu. Le générateur est-il démarré?")
    else:
        print(f"\n📨 Total messages reçus: {message_count}")

except Exception as e:
    print(f"❌ Erreur: {e}")
    print("💡 Vérifiez que Kafka est démarré: docker ps")
