from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

print("🚀 Démarrage du producteur EcoGabes...")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensors = ["GABES_NORD", "GABES_USINE", "GABES_SUD"]
counter = 0

def calculate_aqi(so2, no2, pm2_5):
    # Simplification d'exemple
    return round((so2/200 + no2/200 + pm2_5/150) * 100, 1)

def health_risk(aqi):
    if aqi < 50:
        return "LOW"
    elif aqi < 100:
        return "MODERATE"
    elif aqi < 150:
        return "MEDIUM"
    else:
        return "HIGH"

try:
    while True:
        for sensor in sensors:
            metrics = {
                "so2": round(random.uniform(10, 200), 1),
                "no2": round(random.uniform(5, 100), 1),
                "pm2_5": round(random.uniform(5, 150), 1),
                "temperature": round(random.uniform(15, 35), 1)
            }
            aqi = calculate_aqi(metrics["so2"], metrics["no2"], metrics["pm2_5"])
            data = {
                "sensor_id": sensor,
                "timestamp": datetime.now().isoformat(),
                "metrics": metrics,
                "aqi": aqi,
                "health_risk": health_risk(aqi)
            }
            producer.send('pollution-data', data)
            print(f"📤 {sensor}: SO₂={metrics['so2']} NO₂={metrics['no2']} PM2.5={metrics['pm2_5']} AQI={aqi}")
            counter += 1
        
        print(f"📊 Total: {counter} messages")
        time.sleep(3)

except KeyboardInterrupt:
    print(f"\n🛑 Arrêt. {counter} messages envoyés.")
