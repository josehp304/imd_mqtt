"""
Sensor Alert Dispatcher
-----------------------
For every sensor in the database:
  1. Check whether the sensor's lat/long falls inside the geometry of any
     active CAP alert stored in NeonDB.
  2. If the sensor's topic matches the alert category (e.g. a "rainfall"
     sensor is matched against a "rainfall_floods" alert), publish the full
     CAP alert details to the topic  <sensor_topic>/<sensor_id>
     e.g.  rainfall/20001_0000_62963_01

Run once:  python sensor_alert_dispatcher.py
Run loop:  python sensor_alert_dispatcher.py --loop --interval 300
"""

import argparse
import json
import os
import ssl
import time

import psycopg2
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
BROKER_URL   = os.getenv("BROKER_URL")
BROKER_PORT  = int(os.getenv("BROKER_PORT", 8883))
USERNAME     = os.getenv("USERNAME")
PASSWORD     = os.getenv("PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Maps the sensor topic stored in sensor_status → alert categories it cares about.
# Extend this dict as new sensor types are added.
TOPIC_TO_ALERT_CATEGORIES = {
    "rainfall":      ["rainfall_floods", "cloud_burst"],
    "temperature":   ["frost_cold_wave", "heat_wave"],
    "wind":          ["weather_cyclone", "thunderstorm_lightning"],
    "seismic":       ["earthquake", "tsunami"],
    "soil":          ["landslide", "avalanche"],
    "humidity":      ["drought"],
    "fire":          ["pre_fire"],
    "agriculture":   ["pest_attack"],
    # "all" means the sensor receives every alert regardless of category
    "all":           [],   # handled specially below
}

# ── Database ───────────────────────────────────────────────────────────────────

def connect_db():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)


def ensure_columns(conn):
    """
    Ensure the cap_alerts table has every column this script depends on.
    Safe to run repeatedly – uses ADD COLUMN IF NOT EXISTS.
    """
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE cap_alerts
                ADD COLUMN IF NOT EXISTS alert_category VARCHAR(100);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_alert_category
                ON cap_alerts (alert_category);
        """)
        conn.commit()
    print("✅ cap_alerts schema verified (alert_category column present)")


def fetch_all_sensors(conn):
    """
    Return the latest record for every (sensor_id, topic) pair.
    Skips sensors that have no lat/long.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (sensor_id, topic)
                sensor_id,
                topic,
                latitude,
                longitude,
                raw_data,
                received_at
            FROM sensor_status
            WHERE latitude  IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY sensor_id, topic, received_at DESC;
        """)
        rows = cur.fetchall()

    sensors = []
    for row in rows:
        sensors.append({
            "sensor_id":   row[0],
            "topic":       row[1],
            "latitude":    float(row[2]),
            "longitude":   float(row[3]),
            "raw_data":    row[4],
            "received_at": row[5],
        })
    return sensors


def fetch_alerts_containing_point(conn, lon, lat, alert_categories=None):
    """
    Return every CAP alert whose geometry contains the point (lon, lat).
    Optionally restrict to a list of alert_category values.
    """
    with conn.cursor() as cur:
        if alert_categories:
            cur.execute("""
                SELECT DISTINCT ON (identifier)
                    identifier,
                    alert_category,
                    disaster_type,
                    severity,
                    area_description,
                    warning_message,
                    effective_start_time,
                    effective_end_time,
                    properties,
                    ST_AsGeoJSON(geometry) AS geom_json
                FROM cap_alerts
                WHERE geometry IS NOT NULL
                  AND alert_category = ANY(%s)
                  AND ST_Contains(
                        geometry,
                        ST_SetSRID(ST_Point(%s, %s), 4326)
                      )
                ORDER BY identifier, effective_start_time DESC;
            """, (alert_categories, lon, lat))
        else:
            cur.execute("""
                SELECT DISTINCT ON (identifier)
                    identifier,
                    alert_category,
                    disaster_type,
                    severity,
                    area_description,
                    warning_message,
                    effective_start_time,
                    effective_end_time,
                    properties,
                    ST_AsGeoJSON(geometry) AS geom_json
                FROM cap_alerts
                WHERE geometry IS NOT NULL
                  AND ST_Contains(
                        geometry,
                        ST_SetSRID(ST_Point(%s, %s), 4326)
                      )
                ORDER BY identifier, effective_start_time DESC;
            """, (lon, lat))

        rows = cur.fetchall()

    alerts = []
    for row in rows:
        alerts.append({
            "identifier":          row[0],
            "alert_category":      row[1],
            "disaster_type":       row[2],
            "severity":            row[3],
            "area_description":    row[4],
            "warning_message":     row[5],
            "effective_start_time": str(row[6]) if row[6] else None,
            "effective_end_time":   str(row[7]) if row[7] else None,
            "properties":          row[8],
            "geometry":            json.loads(row[9]) if row[9] else None,
        })
    return alerts


# ── MQTT ───────────────────────────────────────────────────────────────────────

def build_mqtt_client():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    client.username_pw_set(USERNAME, PASSWORD)

    def on_connect(c, userdata, flags, rc, props):
        if rc == 0:
            print("✅ MQTT connected")
        else:
            print(f"❌ MQTT connection failed: rc={rc}")

    def on_disconnect(c, userdata, flags, rc, props):
        if rc != 0:
            print(f"⚠️  MQTT disconnected unexpectedly: rc={rc}")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    return client


def publish_alert_to_sensor(client, sensor_topic, sensor_id, sensor_lat,
                             sensor_lon, alert):
    """
    Publish a CAP alert to  <sensor_topic>/<sensor_id>
    e.g.  rainfall/20001_0000_62963_01
    """
    topic = f"{sensor_topic}/{sensor_id}"
    payload = json.dumps({
        "type": "cap_alert_match",
        "sensor": {
            "sensor_id":  sensor_id,
            "topic":      sensor_topic,
            "latitude":   sensor_lat,
            "longitude":  sensor_lon,
        },
        "alert": {
            "identifier":           alert["identifier"],
            "alert_category":       alert["alert_category"],
            "disaster_type":        alert["disaster_type"],
            "severity":             alert["severity"],
            "area_description":     alert["area_description"],
            "warning_message":      alert["warning_message"],
            "effective_start_time": alert["effective_start_time"],
            "effective_end_time":   alert["effective_end_time"],
            "geometry":             alert["geometry"],
            "properties":           alert["properties"],
        },
    })

    result = client.publish(topic, payload, qos=1)
    return result[0] == 0, topic


# ── Core dispatch logic ────────────────────────────────────────────────────────

def dispatch(conn, mqtt_client):
    print("\n" + "=" * 70)
    print("🚀 Starting sensor → CAP alert dispatch")
    print("=" * 70)

    sensors = fetch_all_sensors(conn)
    if not sensors:
        print("⚠️  No sensors found in database (need lat/long).")
        return

    print(f"📍 {len(sensors)} sensor(s) loaded\n")

    total_published  = 0
    total_no_match   = 0
    total_failed     = 0

    for sensor in sensors:
        sid        = sensor["sensor_id"]
        s_topic    = sensor["topic"]          # e.g. "rainfall"
        lat        = sensor["latitude"]
        lon        = sensor["longitude"]

        # Determine which alert categories this sensor cares about
        if s_topic == "all":
            # "all" sensors receive every alert
            categories = None          # no filter → fetch everything
        elif s_topic in TOPIC_TO_ALERT_CATEGORIES:
            categories = TOPIC_TO_ALERT_CATEGORIES[s_topic]
        else:
            # Unknown topic – match against every alert category
            categories = None

        # Spatial query
        alerts = fetch_alerts_containing_point(conn, lon, lat, categories)

        if not alerts:
            total_no_match += 1
            print(f"  ⚪ {sid:<30} topic={s_topic:<12} ({lat}, {lon}) → no matching alerts")
            continue

        print(f"\n  🎯 {sid:<30} topic={s_topic:<12} ({lat}, {lon})")
        print(f"     {len(alerts)} matching alert(s):")

        for alert in alerts:
            ok, pub_topic = publish_alert_to_sensor(
                mqtt_client, s_topic, sid, lat, lon, alert
            )
            if ok:
                total_published += 1
                print(f"     ✅ → {pub_topic}")
                print(f"        [{alert['alert_category']}] {alert['disaster_type']} "
                      f"| {alert['severity']} | {alert['area_description']}")
            else:
                total_failed += 1
                print(f"     ❌ Failed to publish to {pub_topic}")

    print("\n" + "=" * 70)
    print("📊 Dispatch Summary")
    print(f"  📍 Sensors checked     : {len(sensors)}")
    print(f"  🎯 Alerts published    : {total_published}")
    print(f"  ⚪ Sensors with no match: {total_no_match}")
    print(f"  ❌ Publish failures    : {total_failed}")
    print("=" * 70 + "\n")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sensor Alert Dispatcher – matches sensors to CAP alerts "
                    "and publishes via MQTT."
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Run continuously instead of once."
    )
    parser.add_argument(
        "--interval", type=int, default=300,
        help="Seconds between runs when --loop is active (default: 300)."
    )
    args = parser.parse_args()

    # Connect to MQTT
    print("🔌 Connecting to MQTT broker…")
    mqtt_client = build_mqtt_client()
    mqtt_client.connect(BROKER_URL, BROKER_PORT, keepalive=60)
    mqtt_client.loop_start()
    time.sleep(1)   # allow on_connect to fire

    # Connect to DB
    print("🔌 Connecting to NeonDB…")
    conn = connect_db()
    print("✅ Database connected")
    ensure_columns(conn)
    print()

    try:
        if args.loop:
            print(f"🔄 Loop mode – running every {args.interval}s. "
                  f"Press Ctrl+C to stop.\n")
            while True:
                dispatch(conn, mqtt_client)
                print(f"⏳ Sleeping {args.interval}s…\n")
                time.sleep(args.interval)
        else:
            dispatch(conn, mqtt_client)
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user.")
    finally:
        conn.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("✅ Disconnected cleanly.")


if __name__ == "__main__":
    main()
