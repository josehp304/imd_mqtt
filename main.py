import time
import paho.mqtt.client as mqtt
import ssl
from fetch_alerts import main as fetch_main
import os

# 1. Define Settings

BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = os.getenv("BROKER_PORT")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
geojson_path = os.getenv("GEOJSON_PATH")
# 2. Define Callback Functions
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✅ Connected successfully!")
        # Subscribing in on_connect ensures we resubscribe if connection is lost
        client.subscribe("earthquake") 
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

fetch_main()



with open(geojson_path, "r") as f:
    geo_content = f.read()

try:
    result = client.publish("earthquake",geo_content,qos=1)
    print(result)
    
    if result[0] == 0:
        print("successfully sent new earthquake cap alerts")
    else:
        print("failed to send message")
except Exception as e:
    print(f"message failed: {e}")
    exit(1)

# Keep script running to listen for messages
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
    client.loop_stop()
    client.disconnect()