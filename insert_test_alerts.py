"""
Insert test CAP alerts into NeonDB to verify sensor_alert_dispatcher.py works.

Test alerts are simple rectangular polygons covering each known sensor location:
  Sensor 01 – Nagpur    (lat=21.26,   lon=77.41)   → alert_category=rainfall_floods
  Sensor 02 – Delhi     (lat=28.6139, lon=77.209)  → alert_category=cloud_burst
  Sensor 03 – Mumbai    (lat=19.076,  lon=72.8777) → alert_category=rainfall_floods
  Sensor 04 – Bangalore (lat=12.9716, lon=77.5946) → alert_category=cloud_burst
  Sensor 05 – Kolkata   (lat=22.5726, lon=88.3639) → alert_category=rainfall_floods

All sensors have topic="rainfall" which maps to ["rainfall_floods", "cloud_burst"],
so all five should receive an alert after running sensor_alert_dispatcher.py.

Usage:
    python insert_test_alerts.py          # insert test data
    python insert_test_alerts.py --clean  # remove test data only
"""

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ---------------------------------------------------------------------------
# Test alert definitions
# Each polygon is a simple bounding box that contains the target sensor point.
# Polygon coords: (lon_min lat_min, lon_max lat_min, lon_max lat_max,
#                  lon_min lat_max, lon_min lat_min)   (WKT order = lon lat)
# ---------------------------------------------------------------------------
TEST_ALERTS = [
    {
        "identifier": "TEST-ALERT-NAGPUR-001",
        "alert_category": "rainfall_floods",
        "disaster_type": "Rainfall / Floods",
        "area_description": "Nagpur District – Heavy Rainfall Warning (TEST)",
        "severity": "Severe",
        "severity_color": "Red",
        "warning_message": "TEST: Heavy rainfall expected over Nagpur district.",
        # Bounding box covering sensor 01 (21.26, 77.41)
        "polygon_wkt": "POLYGON((77.0 21.0, 77.9 21.0, 77.9 21.6, 77.0 21.6, 77.0 21.0))",
    },
    {
        "identifier": "TEST-ALERT-DELHI-001",
        "alert_category": "cloud_burst",
        "disaster_type": "Cloud Burst",
        "area_description": "Delhi NCR – Cloud Burst Warning (TEST)",
        "severity": "Extreme",
        "severity_color": "Red",
        "warning_message": "TEST: Cloud burst likely over Delhi and surrounding NCR.",
        # Bounding box covering sensor 02 (28.6139, 77.209)
        "polygon_wkt": "POLYGON((76.8 28.3, 77.7 28.3, 77.7 29.0, 76.8 29.0, 76.8 28.3))",
    },
    {
        "identifier": "TEST-ALERT-MUMBAI-001",
        "alert_category": "rainfall_floods",
        "disaster_type": "Rainfall / Floods",
        "area_description": "Mumbai Metropolitan Region – Flood Warning (TEST)",
        "severity": "Severe",
        "severity_color": "Orange",
        "warning_message": "TEST: Heavy rainfall and flooding expected in Mumbai.",
        # Bounding box covering sensor 03 (19.076, 72.8777)
        "polygon_wkt": "POLYGON((72.6 18.8, 73.3 18.8, 73.3 19.4, 72.6 19.4, 72.6 18.8))",
    },
    {
        "identifier": "TEST-ALERT-BANGALORE-001",
        "alert_category": "cloud_burst",
        "disaster_type": "Cloud Burst",
        "area_description": "Bangalore Urban – Cloud Burst Warning (TEST)",
        "severity": "Moderate",
        "severity_color": "Yellow",
        "warning_message": "TEST: Cloud burst possible over Bangalore urban district.",
        # Bounding box covering sensor 04 (12.9716, 77.5946)
        "polygon_wkt": "POLYGON((77.3 12.7, 77.9 12.7, 77.9 13.3, 77.3 13.3, 77.3 12.7))",
    },
    {
        "identifier": "TEST-ALERT-KOLKATA-001",
        "alert_category": "rainfall_floods",
        "disaster_type": "Rainfall / Floods",
        "area_description": "Kolkata and Howrah – Heavy Rainfall Warning (TEST)",
        "severity": "Severe",
        "severity_color": "Red",
        "warning_message": "TEST: Heavy to very heavy rainfall expected over Kolkata.",
        # Bounding box covering sensor 05 (22.5726, 88.3639)
        "polygon_wkt": "POLYGON((88.1 22.3, 88.7 22.3, 88.7 22.9, 88.1 22.9, 88.1 22.3))",
    },
]

INSERT_SQL = """
INSERT INTO cap_alerts (
    identifier,
    alert_category,
    disaster_type,
    area_description,
    severity,
    severity_color,
    warning_message,
    geometry,
    feature_type
)
VALUES (
    %(identifier)s,
    %(alert_category)s,
    %(disaster_type)s,
    %(area_description)s,
    %(severity)s,
    %(severity_color)s,
    %(warning_message)s,
    ST_SetSRID(ST_GeomFromText(%(polygon_wkt)s), 4326),
    'Feature'
)
ON CONFLICT (identifier) DO UPDATE SET
    alert_category  = EXCLUDED.alert_category,
    disaster_type   = EXCLUDED.disaster_type,
    area_description= EXCLUDED.area_description,
    severity        = EXCLUDED.severity,
    severity_color  = EXCLUDED.severity_color,
    warning_message = EXCLUDED.warning_message,
    geometry        = EXCLUDED.geometry;
"""

DELETE_SQL = "DELETE FROM cap_alerts WHERE identifier LIKE 'TEST-ALERT-%';"


def connect():
    if not DATABASE_URL:
        sys.exit("❌  DATABASE_URL is not set in .env")
    return psycopg2.connect(DATABASE_URL)


def insert_test_data(conn):
    with conn.cursor() as cur:
        for alert in TEST_ALERTS:
            cur.execute(INSERT_SQL, alert)
            print(f"  ✅  Inserted/updated: {alert['identifier']}  ({alert['alert_category']})")
    conn.commit()
    print(f"\n✔  {len(TEST_ALERTS)} test alert(s) written to cap_alerts table.")


def clean_test_data(conn):
    with conn.cursor() as cur:
        cur.execute(DELETE_SQL)
        removed = cur.rowcount
    conn.commit()
    print(f"🗑  Removed {removed} test alert(s) from cap_alerts table.")


def verify(conn):
    """Quick sanity check – print each test alert and whether sensors fall inside it."""
    sql = """
        SELECT
            a.identifier,
            a.alert_category,
            s.sensor_id,
            s.topic,
            s.latitude,
            s.longitude
        FROM cap_alerts a
        JOIN (
            SELECT DISTINCT ON (sensor_id, topic)
                sensor_id,
                topic,
                latitude,
                longitude
            FROM sensor_status
            WHERE latitude  IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY sensor_id, topic, received_at DESC
        ) s
          ON ST_Contains(
                a.geometry,
                ST_SetSRID(ST_Point(s.longitude, s.latitude), 4326)
             )
        WHERE a.identifier LIKE 'TEST-ALERT-%'
        ORDER BY a.identifier, s.sensor_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        print("\n⚠  No spatial matches found – check that sensor_status has rows with lat/lon.")
        return

    print("\n── Spatial match verification ──────────────────────────────")
    print(f"{'Alert identifier':<35} {'category':<20} {'sensor_id':<30} {'topic':<12} lat/lon")
    print("─" * 115)
    for identifier, category, sensor_id, topic, lat, lon in rows:
        print(f"{identifier:<35} {category:<20} {sensor_id:<30} {topic:<12} ({lat}, {lon})")
    print(f"\n✔  {len(rows)} sensor–alert spatial match(es) confirmed.")


def main():
    parser = argparse.ArgumentParser(description="Insert test CAP alerts into NeonDB")
    parser.add_argument("--clean", action="store_true",
                        help="Remove test data instead of inserting it")
    parser.add_argument("--verify", action="store_true",
                        help="Check spatial matches after inserting (default: True unless --clean)")
    args = parser.parse_args()

    conn = connect()
    try:
        if args.clean:
            clean_test_data(conn)
        else:
            print("📝  Inserting test CAP alerts …\n")
            insert_test_data(conn)
            print()
            verify(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
