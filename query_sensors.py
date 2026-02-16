import psycopg2
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")

def connect_to_db():
    """Connect to NeonDB PostgreSQL database"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def get_all_sensors(topic=None):
    """Get all sensors, optionally filtered by topic"""
    conn = connect_to_db()
    with conn.cursor() as cur:
        if topic:
            cur.execute("""
                SELECT DISTINCT ON (sensor_id, topic)
                    sensor_id, topic, latitude, longitude, raw_data, received_at
                FROM sensor_status
                WHERE topic = %s
                ORDER BY sensor_id, topic, received_at DESC;
            """, (topic,))
        else:
            cur.execute("""
                SELECT DISTINCT ON (sensor_id, topic)
                    sensor_id, topic, latitude, longitude, raw_data, received_at
                FROM sensor_status
                ORDER BY sensor_id, topic, received_at DESC;
            """)
        
        results = cur.fetchall()
    conn.close()
    return results

def get_sensors_in_polygon(polygon_coords):
    """
    Get sensors that fall within a polygon
    polygon_coords: List of (longitude, latitude) tuples representing the polygon
    
    Example: [(77.0, 28.0), (77.5, 28.0), (77.5, 28.5), (77.0, 28.5), (77.0, 28.0)]
    """
    conn = connect_to_db()
    with conn.cursor() as cur:
        # Create a WKT polygon string
        polygon_wkt = "POLYGON((" + ", ".join([f"{lon} {lat}" for lon, lat in polygon_coords]) + "))"
        
        cur.execute("""
            SELECT DISTINCT ON (sensor_id, topic)
                sensor_id, topic, latitude, longitude, raw_data, received_at
            FROM sensor_status
            WHERE ST_Contains(
                ST_GeomFromText(%s, 4326),
                ST_Point(longitude, latitude)
            )
            ORDER BY sensor_id, topic, received_at DESC;
        """, (polygon_wkt,))
        
        results = cur.fetchall()
    conn.close()
    return results

def get_recent_sensor_updates(hours=24):
    """Get sensors updated in the last N hours"""
    conn = connect_to_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sensor_id, topic, latitude, longitude, raw_data, received_at
            FROM sensor_status
            WHERE received_at >= NOW() - INTERVAL '%s hours'
            ORDER BY received_at DESC;
        """, (hours,))
        
        results = cur.fetchall()
    conn.close()
    return results

def print_sensors(sensors):
    """Pretty print sensor data"""
    if not sensors:
        print("No sensors found.")
        return
    
    print(f"\n{'='*80}")
    print(f"Found {len(sensors)} sensor(s)")
    print(f"{'='*80}\n")
    
    for sensor in sensors:
        sensor_id, topic, lat, lon, raw_data, received_at = sensor
        print(f"Sensor ID: {sensor_id}")
        print(f"Topic: {topic}")
        print(f"Location: ({lat}, {lon})")
        print(f"Received: {received_at}")
        print(f"Raw Data: {json.dumps(raw_data, indent=2)}")
        print("-" * 80)

if __name__ == "__main__":
    import sys
    
    print("🔍 Querying Sensor Status Database\n")
    
    if len(sys.argv) > 1:
        topic = sys.argv[1]
        print(f"Filtering by topic: {topic}\n")
        sensors = get_all_sensors(topic=topic)
    else:
        print("Getting all sensors (use: python query_sensors.py <topic> to filter)\n")
        sensors = get_all_sensors()
    
    print_sensors(sensors)
