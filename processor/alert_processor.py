import json
import os
import requests
from kafka import KafkaConsumer

# CONFIGURATION
KAFKA_TOPIC = 'raw_telemetry'
KAFKA_BROKER = 'localhost:9092'

# TELEGRAM CONFIGURATION
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TEMP_THRESHOLD = 90.0

def send_telegram_alert(message_text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message_text
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Telegram message: {e}")

# INITIALIZE KAFKA CONSUMER
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening for temperature alerts (>{TEMP_THRESHOLD} C)...")
print("Press Ctrl+C to stop.\n")

try:
    for message in consumer:
        data = message.value
        current_temp = data["temperature"]
        
        # PROCESS ALERTS
        if current_temp > TEMP_THRESHOLD:
            sensor = data["sensor_id"]
            zone = data["factory_zone"]
            
            alert_msg = f"ALERT: {sensor} in {zone} is overheating! Current temperature: {current_temp} C"
            
            print(f"TRIGGERED: {alert_msg}")
            send_telegram_alert(alert_msg)
            
except KeyboardInterrupt:
    print("\nAlert processor stopped by user.")
    consumer.close()