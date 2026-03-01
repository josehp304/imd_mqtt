#!/usr/bin/env python3
"""
Test runner for main.py using mock rainfall alerts.
This script will:
1. Generate test rainfall alerts
2. Store them in the database
3. Check sensors against alert polygons
4. Publish matched alerts to sensor topics
"""

import time
import paho.mqtt.client as mqtt
import ssl
from fetch_alerts_test import main as fetch_test_main
import os
from dotenv import load_dotenv
import json
import psycopg2
from alert_categorizer import categorize_alerts_batch, print_categorization_summary, get_topic_name
from store_to_neondb import store_alerts_to_neondb

# Load environment variables from .env file
load_dotenv()

# Import the sensor checking functions from main.py
import sys
sys.path.insert(0, os.path.dirname(__file__))
from main import get_rainfall_sensors, get_alerts_containing_sensor, publish_alerts_to_sensors

# Settings
BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = os.getenv("BROKER_PORT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "")
geojson_path = "cap_alerts.geojson"
raw_alerts_path = "cap_alerts_raw.json"

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✅ Connected successfully!")
    else:
        print(f"❌ Failed to connect, return code {reason_code}")

def on_message(client, userdata, msg):
    print(f"📩 Received message on topic: {msg.topic}")

print("=" * 80)
print("🧪 RUNNING TEST MODE - Using Mock Rainfall Alerts")
print("=" * 80)
print()

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
client.tls_set(tls_version=ssl.PROTOCOL_TLS)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

# Connect
print("🔌 Connecting to broker...")
try:
    client.connect(BROKER_URL, int(BROKER_PORT), 60)
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

client.loop_start()

# Generate test rainfall alerts
print("\n📥 Generating test rainfall alerts...")
fetch_test_main()

# Store to NeonDB
print("\n💾 Storing test alerts to NeonDB...")
db_result = store_alerts_to_neondb(geojson_path=geojson_path, setup_schema=False)
if db_result["success"]:
    print(f"✅ Database storage complete: {db_result['inserted']} alerts stored")
else:
    print(f"⚠️ Database storage failed: {db_result.get('error', 'Unknown error')}")
    print("Continuing with MQTT publishing...")

# Load raw alerts data
print("\n📂 Loading test alert data...")
with open(raw_alerts_path, "r") as f:
    alerts_data = json.load(f)

# Categorize alerts by type
print("🔍 Categorizing test alerts...")
categorized_alerts = categorize_alerts_batch(alerts_data)
print_categorization_summary(categorized_alerts)

# Publish each category to its own topic
print("📤 Publishing test alerts to categorized topics...")
published_count = 0
failed_count = 0

for alert_type, alerts in categorized_alerts.items():
    topic = get_topic_name(alert_type)
    
    alert_identifiers = {alert.get("identifier") for alert in alerts}
    
    with open(geojson_path, "r") as f:
        full_geojson = json.load(f)
    
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

print("\n" + "=" * 60)
print(f"📊 Publishing Summary:")
print(f"  ✅ Successfully published: {published_count} topics")
print(f"  ❌ Failed: {failed_count} topics")
print(f"  📍 Total alert categories: {len(categorized_alerts)}")
print("=" * 60)

# Check sensors against alert polygons and publish to sensor-specific topics
try:
    publish_alerts_to_sensors(client)
except Exception as e:
    print(f"\n⚠️  Error in sensor-alert matching: {e}")
    import traceback
    traceback.print_exc()
    print("   Test failed!")

print("\n" + "=" * 80)
print("✅ Test completed!")
print("=" * 80)
print("\n💡 Check MQTT topics to verify:")
print("   - alerts/rainfall_floods  (general rainfall alerts)")
print("   - rainfall/<sensor_id>    (sensor-specific alerts)")
print()

# Clean up
time.sleep(2)
client.loop_stop()
client.disconnect()
