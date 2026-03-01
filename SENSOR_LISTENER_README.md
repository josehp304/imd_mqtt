# Sensor Status Listener

This script listens to MQTT sensor status topics and stores sensor location data in the database. This data will later be used to determine which sensors are within alert polygon areas.

## Features

- ✅ Listens to MQTT topic(s) for sensor status messages
- ✅ Extracts sensor ID, latitude, and longitude
- ✅ Stores data in local Docker PostgreSQL with full message payload
- ✅ Supports multiple sensor topics (rainfall, temperature, etc.)
- ✅ Automatic database table creation
- ✅ Indexed for fast spatial queries

## Database Schema

The script creates a `sensor_status` table:

```sql
CREATE TABLE sensor_status (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(255),           -- Unique sensor identifier
    topic VARCHAR(255),                 -- Topic type (e.g., "rainfall")
    latitude DOUBLE PRECISION,          -- Sensor latitude
    longitude DOUBLE PRECISION,         -- Sensor longitude
    raw_data JSONB,                     -- Full message payload
    received_at TIMESTAMP,              -- When message was received
    UNIQUE(sensor_id, topic, received_at)
);
```

## Usage

### 1. Setup

Make sure your `.env` file has the required credentials:

```env
# MQTT Broker
BROKER_URL=your-broker-url.hivemq.cloud
BROKER_PORT=8883
USERNAME=your-username
PASSWORD=your-password

# Local Docker PostgreSQL
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/alertsdb
```

### 2. Run the Listener

```bash
python sensor_status_listener.py
```

The script will:
1. Connect to the MQTT broker
2. Subscribe to `rainfall/status` topic
3. Store sensor data as messages arrive

### 3. Query Sensor Data

To view stored sensor data:

```bash
# Get all sensors
python query_sensors.py

# Get sensors from specific topic
python query_sensors.py rainfall
```

## Adding More Sensor Topics

To listen to additional sensor topics, edit `sensor_status_listener.py`:

```python
topics = [
    "rainfall/status",
    "temperature/status",  # Add new topics here
    "humidity/status",
]
```

## Message Format

The script expects JSON messages with the following fields:

```json
{
    "id": "sensor_123",
    "Lat": 28.6139,
    "Long": 77.2090,
    ... other fields ...
}
```

Field names are case-insensitive. The script will look for:
- `id`, `ID`, or `sensor_id`
- `Lat`, `lat`, or `latitude`
- `Long`, `long`, or `longitude`

## Integration with Alert System

Later, you can query which sensors are within an alert polygon:

```python
from query_sensors import get_sensors_in_polygon

# Define alert polygon coordinates (lon, lat)
polygon = [
    (77.0, 28.0),
    (77.5, 28.0),
    (77.5, 28.5),
    (77.0, 28.5),
    (77.0, 28.0)  # Close the polygon
]

# Get sensors in this area
sensors = get_sensors_in_polygon(polygon)
print(f"Found {len(sensors)} sensors in alert area")
```

## Example Output

```
🚀 Starting Sensor Status Listener
✅ sensor_status table created successfully
🔌 Connecting to broker.hivemq.cloud:8883...
✅ Connected to MQTT broker successfully!
📡 Subscribed to: rainfall/status
👂 Listening for sensor status messages...

📩 Received message on topic: rainfall/status
   Payload: {"id":"20001_0000_62963_01","Lat":21.26,...}
✅ Stored sensor 20001_0000_62963_01 from topic 'rainfall' at (21.26, 77.41)
```

## Troubleshooting

### Database Connection Error
- Verify `DATABASE_URL` is set correctly in `.env`
- Check that your Neon database is active

### MQTT Connection Error
- Verify broker URL, port, username, and password
- Check network connectivity
- Ensure SSL/TLS is properly configured

### No Messages Received
- Verify the sensor is publishing to the correct topic
- Check MQTT broker logs
- Use an MQTT client (like MQTTX) to verify messages are being published
