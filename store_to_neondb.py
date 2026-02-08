import json
import os
import psycopg2
from psycopg2.extras import execute_values

# NeonDB connection string - set this as environment variable or replace directly
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def connect_to_db():
    """Connect to NeonDB PostgreSQL database"""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set. "
                        "Set it to your NeonDB connection string.")
    
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def setup_database(conn):
    """Create the necessary tables and enable PostGIS"""
    with conn.cursor() as cur:
        # Enable PostGIS extension (if not already enabled)
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        
        # Drop existing table if you want to refresh data
        cur.execute("DROP TABLE IF EXISTS earthquake_alerts CASCADE;")
        
        # Create table with geometry column and properties as JSONB
        cur.execute("""
            CREATE TABLE IF NOT EXISTS earthquake_alerts (
                id SERIAL PRIMARY KEY,
                feature_type VARCHAR(50),
                geometry GEOMETRY,
                warning_message TEXT,
                effective_start_time TIMESTAMP,
                depth VARCHAR(20),
                magnitude DECIMAL(3,1),
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                location VARCHAR(255),
                intensity DECIMAL(5,2),
                color VARCHAR(50),
                radius DECIMAL(15,2),
                zone_name VARCHAR(100),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create spatial index for efficient geo queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earthquake_alerts_geometry 
            ON earthquake_alerts USING GIST (geometry);
        """)
        
        # Create index on feature_type for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earthquake_alerts_feature_type 
            ON earthquake_alerts (feature_type);
        """)
        
        # Create index on magnitude for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_earthquake_alerts_magnitude 
            ON earthquake_alerts (magnitude);
        """)
        
        conn.commit()
        print("✅ Database schema created successfully")

def parse_timestamp(time_str):
    """Parse the timestamp string to a format PostgreSQL understands"""
    if not time_str:
        return None
    
    # Example: "Sun Feb 01 10:34:17 IST 2026"
    from datetime import datetime
    try:
        # Remove timezone abbreviation for parsing
        parts = time_str.split()
        if len(parts) >= 5:
            # Reconstruct without timezone: "Feb 01 10:34:17 2026"
            clean_str = f"{parts[1]} {parts[2]} {parts[3]} {parts[5]}"
            return datetime.strptime(clean_str, "%b %d %H:%M:%S %Y")
    except (ValueError, IndexError):
        pass
    return None

def insert_features(conn, geojson_data):
    """Insert GeoJSON features into the database"""
    features = geojson_data.get("features", [])
    
    if not features:
        print("No features to insert")
        return
    
    with conn.cursor() as cur:
        inserted_count = 0
        
        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            
            # Convert geometry to WKT format for PostGIS
            geom_json = json.dumps(geometry)
            
            # Parse timestamp
            timestamp = parse_timestamp(props.get("effective_start_time"))
            
            cur.execute("""
                INSERT INTO earthquake_alerts (
                    feature_type, geometry, warning_message, effective_start_time,
                    depth, magnitude, latitude, longitude, location,
                    intensity, color, radius, zone_name, properties
                ) VALUES (
                    %s, ST_GeomFromGeoJSON(%s), %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """, (
                props.get("feature_type"),
                geom_json,
                props.get("warning_message"),
                timestamp,
                props.get("depth"),
                props.get("magnitude"),
                props.get("latitude"),
                props.get("longitude"),
                props.get("location"),
                props.get("intensity"),
                props.get("color"),
                props.get("radius"),
                props.get("zone_name"),
                json.dumps(props)
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"✅ Inserted {inserted_count} features into database")

def query_sample_data(conn):
    """Query and display sample data to verify insertion"""
    with conn.cursor() as cur:
        # Count total features
        cur.execute("SELECT COUNT(*) FROM earthquake_alerts")
        total = cur.fetchone()[0]
        print(f"\n📊 Total features in database: {total}")
        
        # Count by feature type
        cur.execute("""
            SELECT feature_type, COUNT(*) 
            FROM earthquake_alerts 
            GROUP BY feature_type
        """)
        print("\nFeatures by type:")
        for row in cur.fetchall():
            print(f"  • {row[0]}: {row[1]}")
        
        # List epicenters
        cur.execute("""
            SELECT location, magnitude, latitude, longitude 
            FROM earthquake_alerts 
            WHERE feature_type = 'epicenter'
            ORDER BY magnitude DESC
        """)
        print("\nEpicenter locations:")
        for row in cur.fetchall():
            print(f"  • M{row[1]} - {row[0]} ({row[2]}, {row[3]})")

def main():
    # Load GeoJSON file
    geojson_path = "earthquake_alerts.geojson"
    
    try:
        with open(geojson_path, "r") as f:
            geojson_data = json.load(f)
        print(f"📂 Loaded {len(geojson_data.get('features', []))} features from {geojson_path}")
    except FileNotFoundError:
        print(f"❌ File not found: {geojson_path}")
        print("Run fetch_alerts.py first to generate the GeoJSON file")
        return
    
    # Connect to database
    try:
        print("\n🔌 Connecting to NeonDB...")
        conn = connect_to_db()
        print("✅ Connected to database")
        
        # Setup schema
        print("\n📋 Setting up database schema...")
        setup_database(conn)
        
        # Insert features
        print("\n📥 Inserting GeoJSON features...")
        insert_features(conn, geojson_data)
        
        # Verify data
        print("\n🔍 Verifying inserted data...")
        query_sample_data(conn)
        
        conn.close()
        print("\n✅ Done! Data successfully stored in NeonDB")
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nTo set the DATABASE_URL environment variable:")
        print("  export DATABASE_URL='postgresql://user:password@hostname/database?sslmode=require'")
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    main()
