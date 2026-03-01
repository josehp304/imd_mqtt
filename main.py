import time
import paho.mqtt.client as mqtt
import ssl
from fetch_alerts import main as fetch_main
import os
from dotenv import load_dotenv
import json
import psycopg2
from alert_categorizer import categorize_alerts_batch, print_categorization_summary, get_topic_name
from store_to_neondb import store_alerts_to_neondb

# Load environment variables from .env file
load_dotenv()

# 1. Define Settings

BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = os.getenv("BROKER_PORT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "")
geojson_path="cap_alerts.geojson"
raw_alerts_path="cap_alerts_raw.json"

# 2. Database Helper Functions

def connect_to_db():
    """Connect to NeonDB PostgreSQL database"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)

def get_rainfall_sensors():
    """Get all sensors with topic 'rainfall' from the database"""
    try:
        conn = connect_to_db()
        with conn.cursor() as cur:
            # Get the latest status for each rainfall sensor
            cur.execute("""
                SELECT DISTINCT ON (sensor_id)
                    sensor_id, topic, latitude, longitude, raw_data, received_at
                FROM sensor_status
                WHERE topic = 'rainfall'
                ORDER BY sensor_id, received_at DESC;
            """)
            results = cur.fetchall()
        conn.close()
        
        sensors = []
        for row in results:
            sensors.append({
                'sensor_id': row[0],
                'topic': row[1],
                'latitude': row[2],
                'longitude': row[3],
                'raw_data': row[4],
                'received_at': row[5]
            })
        return sensors
    except Exception as e:
        print(f"⚠️  Error fetching rainfall sensors: {e}")
        return []

def get_alerts_containing_sensor(sensor_lat, sensor_lon):
    """Get CAP alerts whose polygon contains the sensor location"""
    try:
        conn = connect_to_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    identifier, 
                    disaster_type, 
                    severity,
                    area_description,
                    warning_message,
                    effective_start_time,
                    effective_end_time,
                    properties,
                    ST_AsGeoJSON(geometry) as geometry_geojson
                FROM cap_alerts
                WHERE geometry IS NOT NULL
                AND ST_Contains(
                    geometry,
                    ST_SetSRID(ST_Point(%s, %s), 4326)
                )
                ORDER BY effective_start_time DESC;
            """, (sensor_lon, sensor_lat))
            
            results = cur.fetchall()
        conn.close()
        
        alerts = []
        for row in results:
            alerts.append({
                'identifier': row[0],
                'disaster_type': row[1],
                'severity': row[2],
                'area_description': row[3],
                'warning_message': row[4],
                'effective_start_time': str(row[5]) if row[5] else None,
                'effective_end_time': str(row[6]) if row[6] else None,
                'properties': row[7],
                'geometry': json.loads(row[8]) if row[8] else None
            })
        return alerts
    except Exception as e:
        print(f"⚠️  Error checking alerts for sensor: {e}")
        return []

def publish_alerts_to_sensors(client):
    """Check sensors against CAP alerts and publish to sensor-specific topics"""
    print("\n" + "="*70)
    print("🌧️  Checking rainfall sensors against CAP alert polygons...")
    print("="*70)
    
    # Get all rainfall sensors
    sensors = get_rainfall_sensors()
    
    if not sensors:
        print("⚠️  No rainfall sensors found in database")
        print("   Make sure sensor_status_listener.py is running and receiving data")
        return
    
    print(f"📍 Found {len(sensors)} rainfall sensor(s)")
    
    matched_sensors = 0
    total_alerts_sent = 0
    
    for sensor in sensors:
        sensor_id = sensor['sensor_id']
        lat = sensor['latitude']
        lon = sensor['longitude']
        
        # Find all alerts that contain this sensor
        alerts = get_alerts_containing_sensor(lat, lon)
        
        if alerts:
            matched_sensors += 1
            print(f"\n  🎯 Sensor {sensor_id} at ({lat}, {lon})")
            print(f"     Found {len(alerts)} matching alert(s)")
            
            for alert in alerts:
                # Prepare alert message
                alert_message = {
                    "type": "cap_alert",
                    "sensor_id": sensor_id,
                    "sensor_location": {
                        "latitude": lat,
                        "longitude": lon
                    },
                    "alert": {
                        "identifier": alert['identifier'],
                        "disaster_type": alert['disaster_type'],
                        "severity": alert['severity'],
                        "area_description": alert['area_description'],
                        "warning_message": alert['warning_message'],
                        "effective_start_time": alert['effective_start_time'],
                        "effective_end_time": alert['effective_end_time'],
                        "geometry": alert['geometry']
                    }
                }
                
                # Publish to sensor-specific topic
                topic = f"rainfall/{sensor_id}"
                payload = json.dumps(alert_message)
                
                try:
                    result = client.publish(topic, payload, qos=1)
                    if result[0] == 0:
                        total_alerts_sent += 1
                        print(f"     ✅ Published alert to '{topic}'")
                        print(f"        Alert: {alert['disaster_type']} - {alert['severity']}")
                    else:
                        print(f"     ❌ Failed to publish to '{topic}' (error: {result[0]})")
                except Exception as e:
                    print(f"     ❌ Exception publishing to '{topic}': {e}")
        else:
            print(f"  ⚪ Sensor {sensor_id}: No matching alerts")
    
    print("\n" + "="*70)
    print(f"📊 Sensor-Alert Matching Summary:")
    print(f"  📍 Total rainfall sensors: {len(sensors)}")
    print(f"  🎯 Sensors with matching alerts: {matched_sensors}")
    print(f"  📤 Total alerts sent to sensors: {total_alerts_sent}")
    print("="*70)

# 3. Define Callback Functions
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✅ Connected successfully!")
        # Subscribe to all alert topics
        topics = [
            "alerts/weather_cyclone",
            "alerts/rainfall_floods",
            "alerts/thunderstorm_lightning",
            "alerts/hailstorm",
            "alerts/cloud_burst",
            "alerts/frost_cold_wave",
            "alerts/earthquake",
            "alerts/tsunami",
            "alerts/landslide",
            "alerts/avalanche",
            "alerts/drought",
            "alerts/pre_fire",
            "alerts/pest_attack",
            "alerts/other"
        ]
        for topic in topics:
            client.subscribe(topic)
        print(f"📡 Subscribed to {len(topics)} alert topics")
    else:
        print(f"❌ Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    print(f"📩 Received message: {msg.payload.decode()} on topic: {msg.topic}")

# 3. Create Client & Configure TLS

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

client.tls_set(tls_version=ssl.PROTOCOL_TLS)

# Set credentials
client.username_pw_set(USERNAME, PASSWORD)

# Attach callbacks
client.on_connect = on_connect
client.on_message = on_message

# 4. Connect
print("Connecting to broker...")
try:
    client.connect(BROKER_URL, int(BROKER_PORT), 60)
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

# 5. Start the Loop (Handles network traffic in background)
client.loop_start()

# Fetch all alerts
print("📥 Fetching alerts from NDMA...")
fetch_main()

# Load raw alerts data
print("\n📂 Loading alert data...")
with open(raw_alerts_path, "r") as f:
    alerts_data = json.load(f)

# Categorize alerts by type
print("🔍 Categorizing alerts...")
categorized_alerts = categorize_alerts_batch(alerts_data)
print_categorization_summary(categorized_alerts)

# Build a lookup: identifier -> alert_category
identifier_to_category = {}
for alert_type, alerts in categorized_alerts.items():
    for alert in alerts:
        identifier_to_category[alert.get("identifier", "")] = alert_type

# Tag every GeoJSON feature with its alert_category before storing
print("🏷️  Tagging GeoJSON features with alert categories...")
with open(geojson_path, "r") as f:
    full_geojson = json.load(f)

for feature in full_geojson.get("features", []):
    ident = feature.get("properties", {}).get("identifier", "")
    feature["properties"]["alert_category"] = identifier_to_category.get(ident, "other")

# Overwrite the GeoJSON file with the tagged version so store_to_neondb picks it up
with open(geojson_path, "w") as f:
    json.dump(full_geojson, f)

# Store to NeonDB right after fetching & categorizing
print("\n💾 Storing alerts to NeonDB...")
db_result = store_alerts_to_neondb(geojson_data=full_geojson, setup_schema=False)
if db_result["success"]:
    print(f"✅ NeonDB: {db_result['inserted']} upserted, {db_result.get('skipped', 0)} skipped")
else:
    print(f"⚠️ Database storage failed: {db_result.get('error', 'Unknown error')}")
    print("Continuing with MQTT publishing...")

# Publish each category to its own topic
print("📤 Publishing alerts to categorized topics...")
published_count = 0
failed_count = 0

for alert_type, alerts in categorized_alerts.items():
    topic = get_topic_name(alert_type)

    # Filter features for this category using the already-loaded full_geojson
    alert_identifiers = {alert.get("identifier") for alert in alerts}

    # Filter features for this category
    category_features = [
        feature for feature in full_geojson.get("features", [])
        if feature.get("properties", {}).get("identifier") in alert_identifiers
    ]
    
    category_geojson = {
        "type": "FeatureCollection",
        "features": category_features,
        "metadata": {
            "alert_type": alert_type,
            "alert_count": len(alerts),
            "topic": topic
        }
    }
    
    # Publish to MQTT
    payload = json.dumps(category_geojson)
    
    try:
        result = client.publish(topic, payload, qos=1)
        
        if result[0] == 0:
            published_count += 1
            print(f"  ✅ {topic:<30} : {len(alerts)} alerts published")
        else:
            failed_count += 1
            print(f"  ❌ {topic:<30} : Failed to publish (error code: {result[0]})")
    except Exception as e:
        failed_count += 1
        print(f"  ❌ {topic:<30} : Exception: {e}")

print("\n" + "="*60)
print(f"📊 Publishing Summary:")
print(f"  ✅ Successfully published: {published_count} topics")
print(f"  ❌ Failed: {failed_count} topics")
print(f"  📍 Total alert categories: {len(categorized_alerts)}")
print("="*60)

# Check sensors against alert polygons and publish to sensor-specific topics
try:
    publish_alerts_to_sensors(client)
except Exception as e:
    print(f"\n⚠️  Error in sensor-alert matching: {e}")
    print("   Continuing with normal operation...")

# Keep script running to listen for messages
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
    client.loop_stop()
    client.disconnect()