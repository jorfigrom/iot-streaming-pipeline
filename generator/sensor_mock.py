import json
import time
import random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

# Initialize Faker for synthetic data generation
fake = Faker()

# 1. KAFKA CONNECTION
# Pointing to the local Kafka broker
# value_serializer converts the Python dictionary to a JSON string encoded in UTF-8
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'raw_telemetry'

print("Starting factory machinery simulation...")
print("Press Ctrl+C to stop the generator.\n")

try:
    while True:
        # 2. GENERATE SENSOR DATA
        # Simulating factory machines (MACHINE-10 to MACHINE-50)
        # Temperature in Celsius. Values > 85.0 will be our future alerts
        # Pressure in PSI
        data = {
            "sensor_id": f"MACHINE-{random.randint(10, 50)}",
            "temperature": round(random.uniform(50.0, 105.0), 2),
            "pressure": round(random.uniform(100.0, 150.0), 2),
            "factory_zone": random.choice(["Sector_A", "Sector_B", "Sector_C", "Sector_D"]),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }

        # 3. SEND TO KAFKA
        producer.send(TOPIC, value=data)
        
        # Print to console for local monitoring
        print(f"Sent to Kafka -> {data}")

        # Wait 2 seconds before the next reading
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nSimulation stopped by user.")
    producer.close()