import paho.mqtt.client as mqtt
import ssl
import json
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MQTT Configuration
BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = int(os.getenv("BROKER_PORT", 8883))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "")

def connect_to_db():
    """Connect to NeonDB PostgreSQL database"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def setup_sensor_status_table():
    """Create the sensor_status table if it doesn't exist"""
    conn = connect_to_db()
    with conn.cursor() as cur:
        # Create table for sensor status
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_status (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(255),
                topic VARCHAR(255),
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                raw_data JSONB,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sensor_id, topic, received_at)
            );
        """)
        
        # Create indexes for faster queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_status(sensor_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_topic ON sensor_status(topic);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_received_at ON sensor_status(received_at);
        """)
        
        conn.commit()
        print("✅ sensor_status table created successfully")
    conn.close()

def extract_sensor_data(payload):
    """Extract id, latitude, and longitude from the sensor message"""
    try:
        data = json.loads(payload)
        
        # Extract fields (handle different possible field names)
        sensor_id = data.get("id") or data.get("ID") or data.get("sensor_id")
        latitude = data.get("Lat") or data.get("lat") or data.get("latitude")
        longitude = data.get("Long") or data.get("long") or data.get("longitude")
        
        # Convert to float if they're strings
        if latitude:
            latitude = float(latitude)
        if longitude:
            longitude = float(longitude)
        
        return sensor_id, latitude, longitude, data
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        return None, None, None, None
    except Exception as e:
        print(f"❌ Error extracting sensor data: {e}")
        return None, None, None, None

def store_sensor_status(sensor_id, topic, latitude, longitude, raw_data):
    """Store sensor status in the database"""
    try:
        conn = connect_to_db()
        with conn.cursor() as cur:
            # Extract topic type (e.g., "rainfall" from "rainfall/status")
            topic_type = topic.split('/')[0] if '/' in topic else topic
            
            cur.execute("""
                INSERT INTO sensor_status 
                (sensor_id, topic, latitude, longitude, raw_data, received_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sensor_id, topic, received_at) DO UPDATE
                SET latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    raw_data = EXCLUDED.raw_data;
            """, (sensor_id, topic_type, latitude, longitude, json.dumps(raw_data), datetime.now()))
            
            conn.commit()
            print(f"✅ Stored sensor {sensor_id} from topic '{topic_type}' at ({latitude}, {longitude})")
        conn.close()
    except Exception as e:
        print(f"❌ Error storing sensor data: {e}")

# MQTT Callback Functions
def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker"""
    if reason_code == 0:
        print("✅ Connected to MQTT broker successfully!")
        
        # Subscribe to sensor status topics
        topics = [
            "rainfall/status",
            # Add more sensor topics here as needed
            # "temperature/status",
            # "humidity/status",
        ]
        
        for topic in topics:
            client.subscribe(topic)
            print(f"📡 Subscribed to: {topic}")
    else:
        print(f"❌ Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    """Callback when a message is received"""
    try:
        topic = msg.topic
        payload = msg.payload.decode()
        
        print(f"\n📩 Received message on topic: {topic}")
        print(f"   Payload: {payload[:200]}..." if len(payload) > 200 else f"   Payload: {payload}")
        
        # Extract sensor data
        sensor_id, latitude, longitude, raw_data = extract_sensor_data(payload)
        
        if sensor_id and latitude and longitude:
            # Store in database
            store_sensor_status(sensor_id, topic, latitude, longitude, raw_data)
        else:
            print(f"⚠️  Could not extract required fields (id, lat, long) from message")
            
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def on_disconnect(client, userdata, flags, reason_code, properties):
    """Callback when disconnected from MQTT broker"""
    print(f"⚠️  Disconnected from broker with code: {reason_code}")
    if reason_code != 0:
        print("   Attempting to reconnect...")

def main():
    """Main function to run the sensor status listener"""
    print("=" * 60)
    print("🚀 Starting Sensor Status Listener")
    print("=" * 60)
    
    # Setup database table
    try:
        setup_sensor_status_table()
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        print("   Please check your DATABASE_URL configuration")
        return
    
    # Create MQTT client (using MQTTv5)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    # Set username and password
    client.username_pw_set(USERNAME, PASSWORD)
    
    # Configure TLS/SSL
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
    
    # Connect to broker
    print(f"🔌 Connecting to {BROKER_URL}:{BROKER_PORT}...")
    client.connect(BROKER_URL, BROKER_PORT, keepalive=60)
    
    # Start the loop (blocking)
    print("👂 Listening for sensor status messages...")
    print("   Press Ctrl+C to stop\n")
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping sensor status listener...")
        client.disconnect()
        print("✅ Disconnected successfully")

if __name__ == "__main__":
    main()
