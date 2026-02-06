from paho.mqtt import client as mqtt_client
import random
import json

BROKER = "mqtt://broker.hivemq.com"     # replace with your broker
PORT = 1883
TOPIC = "JOSE/status"          # example: "cap/alerts/india"
CLIENT_ID = f"python-mqtt-{random.randint(0, 1000)}"
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        client.subscribe(TOPIC)
    else:
        print("❌ Failed to connect, return code:", rc)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
    except:
        data = msg.payload.decode()
    print(f"[{msg.topic}] {data}")

client = mqtt_client.Client(
    client_id=CLIENT_ID,
    callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
    protocol=mqtt_client.MQTTv5  # use MQTTv5 (newer)
)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_forever()
