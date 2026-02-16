import paho.mqtt.client as mqtt
import ssl
import json
import os
import time
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

# MQTT Configuration
BROKER_URL = os.getenv("BROKER_URL")
BROKER_PORT = int(os.getenv("BROKER_PORT", 8883))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Test sensor data (mimicking the format from the screenshot)
TEST_SENSORS = [
    {
        "id": "20001_0000_62963_01",
        "Lat": "21.26",
        "Long": "77.41",
        "node_ver": "PR_1",
        "node_bat": "3.6",
        "node_ws": "TP_1",
        "Elect": "0",
        "EXTIP": "0",
        "TRIP": "0",
        "Flash": "0",
        "EXTIP1": "0",
        "TRIP1": "0",
        "type": "***",
        "Pres2": "***",
        "Temp1": "***",
        "Temp2": "***",
        "uptime_sec": "162",
        "free_heap": "156356",
        "ssid": "-52",
        "ip": "192.168.3.53"
    },
    {
        "id": "20001_0000_62963_02",
        "Lat": "28.6139",
        "Long": "77.2090",
        "node_ver": "PR_1",
        "node_bat": "3.7",
        "node_ws": "TP_1",
        "Elect": "1",
        "EXTIP": "0",
        "TRIP": "0",
        "Flash": "0",
        "EXTIP1": "0",
        "TRIP1": "0",
        "type": "***",
        "Pres2": "***",
        "Temp1": "25.5",
        "Temp2": "26.0",
        "uptime_sec": "320",
        "free_heap": "158392",
        "ssid": "-48",
        "ip": "192.168.3.54"
    },
    {
        "id": "20001_0000_62963_03",
        "Lat": "19.0760",
        "Long": "72.8777",
        "node_ver": "PR_2",
        "node_bat": "3.8",
        "node_ws": "TP_2",
        "Elect": "0",
        "EXTIP": "1",
        "TRIP": "0",
        "Flash": "1",
        "EXTIP1": "0",
        "TRIP1": "0",
        "type": "***",
        "Pres2": "***",
        "Temp1": "28.2",
        "Temp2": "27.8",
        "uptime_sec": "450",
        "free_heap": "160124",
        "ssid": "-55",
        "ip": "192.168.3.55"
    },
    {
        "id": "20001_0000_62963_04",
        "Lat": "12.9716",
        "Long": "77.5946",
        "node_ver": "PR_1",
        "node_bat": "3.5",
        "node_ws": "TP_1",
        "Elect": "1",
        "EXTIP": "0",
        "TRIP": "1",
        "Flash": "0",
        "EXTIP1": "1",
        "TRIP1": "0",
        "type": "***",
        "Pres2": "***",
        "Temp1": "22.1",
        "Temp2": "23.0",
        "uptime_sec": "600",
        "free_heap": "157890",
        "ssid": "-50",
        "ip": "192.168.3.56"
    },
    {
        "id": "20001_0000_62963_05",
        "Lat": "22.5726",
        "Long": "88.3639",
        "node_ver": "PR_2",
        "node_bat": "3.9",
        "node_ws": "TP_3",
        "Elect": "0",
        "EXTIP": "0",
        "TRIP": "0",
        "Flash": "0",
        "EXTIP1": "0",
        "TRIP1": "0",
        "type": "***",
        "Pres2": "***",
        "Temp1": "30.5",
        "Temp2": "29.8",
        "uptime_sec": "180",
        "free_heap": "159234",
        "ssid": "-45",
        "ip": "192.168.3.57"
    }
]

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker"""
    if reason_code == 0:
        print("✅ Connected to MQTT broker successfully!")
    else:
        print(f"❌ Failed to connect, return code {reason_code}")

def on_publish(client, userdata, mid, reason_code, properties):
    """Callback when message is published"""
    print(f"   ✓ Message published (mid: {mid})")

def publish_test_message(client, topic, sensor_data):
    """Publish a test sensor message"""
    payload = json.dumps(sensor_data)
    result = client.publish(topic, payload, qos=1)
    
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"📤 Publishing to '{topic}':")
        print(f"   Sensor ID: {sensor_data['id']}")
        print(f"   Location: ({sensor_data['Lat']}, {sensor_data['Long']})")
    else:
        print(f"❌ Failed to publish message, error code: {result.rc}")
    
    return result

def generate_random_sensor():
    """Generate a random sensor status message"""
    return {
        "id": f"20001_0000_{random.randint(10000, 99999)}_{random.randint(1, 99):02d}",
        "Lat": f"{random.uniform(8.0, 35.0):.4f}",  # India latitude range
        "Long": f"{random.uniform(68.0, 97.0):.4f}",  # India longitude range
        "node_ver": random.choice(["PR_1", "PR_2", "PR_3"]),
        "node_bat": f"{random.uniform(3.0, 4.2):.1f}",
        "node_ws": random.choice(["TP_1", "TP_2", "TP_3"]),
        "Elect": str(random.randint(0, 1)),
        "EXTIP": str(random.randint(0, 1)),
        "TRIP": str(random.randint(0, 1)),
        "Flash": str(random.randint(0, 1)),
        "EXTIP1": str(random.randint(0, 1)),
        "TRIP1": str(random.randint(0, 1)),
        "type": "***",
        "Pres2": "***",
        "Temp1": f"{random.uniform(15.0, 35.0):.1f}",
        "Temp2": f"{random.uniform(15.0, 35.0):.1f}",
        "uptime_sec": str(random.randint(100, 1000)),
        "free_heap": str(random.randint(150000, 165000)),
        "ssid": str(random.randint(-60, -40)),
        "ip": f"192.168.3.{random.randint(50, 100)}"
    }

def main():
    """Main function to publish test messages"""
    print("=" * 70)
    print("🧪 Sensor Status Test Publisher")
    print("=" * 70)
    print()
    
    # Create MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    # Set username and password
    client.username_pw_set(USERNAME, PASSWORD)
    
    # Configure TLS/SSL
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
    
    # Connect to broker
    print(f"🔌 Connecting to {BROKER_URL}:{BROKER_PORT}...")
    client.connect(BROKER_URL, BROKER_PORT, keepalive=60)
    
    # Start the loop in background
    client.loop_start()
    time.sleep(2)  # Wait for connection
    
    print("\n" + "=" * 70)
    print("Choose an option:")
    print("=" * 70)
    print("1. Publish predefined test sensors (5 sensors)")
    print("2. Publish random sensor data")
    print("3. Publish continuous random data (Ctrl+C to stop)")
    print("4. Publish single custom message")
    print("=" * 70)
    
    try:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            # Publish predefined test sensors
            print(f"\n📡 Publishing {len(TEST_SENSORS)} test sensors to 'rainfall/status'...\n")
            for i, sensor in enumerate(TEST_SENSORS, 1):
                print(f"[{i}/{len(TEST_SENSORS)}]")
                publish_test_message(client, "rainfall/status", sensor)
                time.sleep(1)  # Wait between messages
            
            print(f"\n✅ Published {len(TEST_SENSORS)} test messages successfully!")
        
        elif choice == "2":
            # Publish random sensor
            count = int(input("How many random sensors to publish? "))
            print(f"\n📡 Publishing {count} random sensors to 'rainfall/status'...\n")
            for i in range(count):
                print(f"[{i+1}/{count}]")
                sensor = generate_random_sensor()
                publish_test_message(client, "rainfall/status", sensor)
                time.sleep(1)
            
            print(f"\n✅ Published {count} random messages successfully!")
        
        elif choice == "3":
            # Continuous publishing
            interval = float(input("Publish interval in seconds (e.g., 5): "))
            print(f"\n📡 Publishing random sensors every {interval}s to 'rainfall/status'...")
            print("   Press Ctrl+C to stop\n")
            
            count = 0
            while True:
                count += 1
                print(f"[Message #{count}]")
                sensor = generate_random_sensor()
                publish_test_message(client, "rainfall/status", sensor)
                time.sleep(interval)
        
        elif choice == "4":
            # Custom message
            sensor_id = input("Sensor ID: ")
            lat = input("Latitude: ")
            lon = input("Longitude: ")
            
            sensor = {
                "id": sensor_id,
                "Lat": lat,
                "Long": lon,
                "node_ver": "PR_1",
                "node_bat": "3.7",
                "type": "***"
            }
            
            print(f"\n📡 Publishing custom sensor to 'rainfall/status'...\n")
            publish_test_message(client, "rainfall/status", sensor)
            print("\n✅ Published custom message successfully!")
        
        else:
            print("❌ Invalid choice!")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping publisher...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        time.sleep(1)  # Wait for last message to be sent
        client.loop_stop()
        client.disconnect()
        print("✅ Disconnected from broker")

if __name__ == "__main__":
    main()
