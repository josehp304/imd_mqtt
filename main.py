import time
import paho.mqtt.client as mqtt
import ssl
from fetch_alerts import main as fetch_main
import os
from dotenv import load_dotenv
import json
from alert_categorizer import categorize_alerts_batch, print_categorization_summary, get_topic_name
from store_to_neondb import store_alerts_to_neondb

# Load environment variables from .env file
load_dotenv()

# 1. Define Settings

BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = os.getenv("BROKER_PORT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
geojson_path="cap_alerts.geojson"
raw_alerts_path="cap_alerts_raw.json"
# 2. Define Callback Functions
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

# Store to NeonDB
print("\n💾 Storing alerts to NeonDB...")
db_result = store_alerts_to_neondb(geojson_path=geojson_path, setup_schema=False)
if db_result["success"]:
    print(f"✅ Database storage complete: {db_result['inserted']} alerts stored")
else:
    print(f"⚠️ Database storage failed: {db_result.get('error', 'Unknown error')}")
    print("Continuing with MQTT publishing...")

# Load raw alerts data
print("\n📂 Loading alert data...")
with open(raw_alerts_path, "r") as f:
    alerts_data = json.load(f)

# Categorize alerts by type
print("🔍 Categorizing alerts...")
categorized_alerts = categorize_alerts_batch(alerts_data)
print_categorization_summary(categorized_alerts)

# Publish each category to its own topic
print("📤 Publishing alerts to categorized topics...")
published_count = 0
failed_count = 0

for alert_type, alerts in categorized_alerts.items():
    topic = get_topic_name(alert_type)
    
    # Create GeoJSON for this category
    # Filter features from the full geojson that match these alert identifiers
    alert_identifiers = {alert.get("identifier") for alert in alerts}
    
    with open(geojson_path, "r") as f:
        full_geojson = json.load(f)
    
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

# Keep script running to listen for messages
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
    client.loop_stop()
    client.disconnect()