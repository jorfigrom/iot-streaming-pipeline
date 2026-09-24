import json
import os
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# CONFIGURATION
KAFKA_TOPIC = 'raw_telemetry'
KAFKA_BROKER = 'localhost:9092'

INFLUX_URL = 'http://localhost:8086'
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = 'iot_org'
INFLUX_BUCKET = 'telemetry'

# 1. INITIALIZE INFLUXDB CLIENT
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 2. INITIALIZE KAFKA CONSUMER
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening to Kafka and writing to InfluxDB...")
print("Press Ctrl+C to stop.\n")

try:
    for message in consumer:
        data = message.value
        
        # 3. FORMAT DATA AS INFLUXDB POINT
        point = Point("factory_sensors") \
            .tag("sensor_id", data["sensor_id"]) \
            .tag("factory_zone", data["factory_zone"]) \
            .field("temperature", data["temperature"]) \
            .field("pressure", data["pressure"])
        
        # 4. WRITE TO DATABASE
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        
        print(f"Saved to DB: {data['sensor_id']} | Temp: {data['temperature']} | Pressure: {data['pressure']}")
        
except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
    client.close()
    consumer.close()