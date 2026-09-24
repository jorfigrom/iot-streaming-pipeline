# 🏭 IoT Real-Time Data Streaming Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=InfluxDB&logoColor=white)
![Grafana](https://img.shields.io/badge/grafana-%23F46800.svg?style=for-the-badge&logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

A Proof of Concept (PoC) demonstrating an **Event-Driven Architecture (EDA)** for industrial IoT sensors. This project simulates real-time factory machinery telemetry, streams the data through a message broker, stores it in a time-series database for live visualization, and processes events on the fly to trigger critical alerts.

## 🏗️ Architecture & Data Flow

The system is decoupled into independent microservices communicating via Apache Kafka:

1. **Data Generator (`sensor_mock.py`):** Simulates 40+ industrial machines sending temperature and pressure metrics every 2 seconds.
2. **Message Broker (Apache Kafka - KRaft mode):** Acts as the central nervous system, receiving raw data in the `raw_telemetry` topic.
3. **Data Ingestion (`kafka_to_influx.py`):** Consumes messages from Kafka and persists them efficiently into InfluxDB.
4. **Real-Time Monitoring (Grafana):** Connects to InfluxDB to visualize factory metrics via live dashboards using Flux queries.
5. **Streaming Processor (`alert_processor.py`):** Consumes data on the fly, detects temperature anomalies (e.g., > 90ºC), and immediately pushes an alert to a mobile device via the Telegram API.

## 🛠️ Tech Stack

* **Programming Language:** Python 3
* **Message Broker:** Apache Kafka (confluentinc/cp-kafka)
* **Time-Series Database:** InfluxDB 2.7
* **Data Visualization:** Grafana
* **Infrastructure:** Docker & Docker Compose
* **External APIs:** Telegram Bot API

## 🚀 Getting Started

### Prerequisites
* [Docker & Docker Desktop](https://www.docker.com/)
* [Python 3.8+](https://www.python.org/)
* Git

### Installation & Setup

**1. Clone the repository**
```bash
git clone https://github.com/your-username/iot-streaming-pipeline.git
cd iot-streaming-pipeline
```

**2. Configure Environment Variables**
Copy the example environment file and add your actual API tokens:
```bash
cp .env.example .env
```
*Note: You will need to generate a Telegram Bot Token via BotFather, your personal Telegram Chat ID, and an InfluxDB API Token (generated in step 4).*

**3. Spin up the Infrastructure**
Start Kafka, InfluxDB, and Grafana using Docker Compose:
```bash
cd infra
docker compose up -d
cd ..
```

**4. Set up Python Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
```

## ⚙️ Running the Pipeline

To see the system in action, open three separate terminal windows (ensure the virtual environment is activated in all of them) and run the components in parallel:

**Terminal 1: Start the machinery simulation**
```bash
python generator/sensor_mock.py
```

**Terminal 2: Start the InfluxDB consumer**
```bash
python processor/kafka_to_influx.py
```

**Terminal 3: Start the real-time alerting system**
```bash
python processor/alert_processor.py
```

## 📊 Grafana Dashboard Setup

1. Navigate to `http://localhost:3000` (User: `admin`, Password: `admin`).
2. Add a new Data Source -> InfluxDB.
3. Set Query Language to **Flux**.
4. Set URL to `http://influxdb:8086` (utilizing Docker's internal DNS).
5. Enter your Organization (`iot_org`), Default Bucket (`telemetry`), and your InfluxDB Token.
6. Create a new dashboard and use the following Flux query to visualize temperature:

```flux
from(bucket: "telemetry")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "factory_sensors")
  |> filter(fn: (r) => r["_field"] == "temperature")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "mean")
```

## 📱 Screenshots

* <img width="1252" height="796" alt="image" src="https://github.com/user-attachments/assets/eb44912d-06a4-4f37-95d3-bae93cea2904" />

* <img width="475" height="475" alt="image" src="https://github.com/user-attachments/assets/fa01131b-5089-4224-a9a8-1f24031b9a78" />
