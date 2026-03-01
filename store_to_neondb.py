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
        cur.execute("DROP TABLE IF EXISTS cap_alerts CASCADE;")
        
        # Create table with geometry column and properties as JSONB
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cap_alerts (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(255) UNIQUE,
                alert_category VARCHAR(100),
                feature_type VARCHAR(50),
                geometry GEOMETRY,
                severity VARCHAR(50),
                effective_start_time TIMESTAMP,
                effective_end_time TIMESTAMP,
                disaster_type VARCHAR(100),
                area_description TEXT,
                severity_level TEXT,
                type VARCHAR(50),
                actual_lang VARCHAR(10),
                warning_message TEXT,
                disseminated VARCHAR(20),
                severity_color VARCHAR(50),
                alert_id_sdma_autoinc VARCHAR(50),
                centroid VARCHAR(255),
                alert_source VARCHAR(255),
                area_covered VARCHAR(50),
                sender_org_id VARCHAR(50),
                polygon_area_covered VARCHAR(50),
                min_lat VARCHAR(20),
                max_lat VARCHAR(20),
                min_long VARCHAR(20),
                max_long VARCHAR(20),
                depth VARCHAR(50),
                intensity DECIMAL,
                color VARCHAR(50),
                latitude DECIMAL,
                longitude DECIMAL,
                radius DECIMAL,
                zone_name VARCHAR(255),
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create spatial index for efficient geo queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_geometry 
            ON cap_alerts USING GIST (geometry);
        """)
        
        # Create index on feature_type for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_feature_type 
            ON cap_alerts (feature_type);
        """)
        
        # Create index on disaster_type for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_disaster_type 
            ON cap_alerts (disaster_type);
        """)
        
        # Create index on severity for filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_severity 
            ON cap_alerts (severity);
        """)
        
        # Create index on identifier for lookups
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_identifier 
            ON cap_alerts (identifier);
        """)

        # Create index on alert_category for topic-based filtering
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cap_alerts_alert_category 
            ON cap_alerts (alert_category);
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
        skipped_count = 0
        
        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            
            # Convert geometry to WKT format for PostGIS (handle null geometry)
            geom_json = json.dumps(geometry) if geometry else None
            
            # Parse timestamps
            effective_start = parse_timestamp(props.get("effective_start_time"))
            effective_end = parse_timestamp(props.get("effective_end_time"))
            
            try:
                cur.execute("""
                    INSERT INTO cap_alerts (
                        identifier, alert_category, feature_type, geometry, severity, effective_start_time,
                        effective_end_time, disaster_type, area_description, severity_level,
                        type, actual_lang, warning_message, disseminated, severity_color,
                        alert_id_sdma_autoinc, centroid, alert_source, area_covered,
                        sender_org_id, polygon_area_covered, min_lat, max_lat, min_long, max_long,
                        depth, intensity, color, latitude, longitude, radius, zone_name, properties
                    ) VALUES (
                        %s, %s, %s, ST_GeomFromGeoJSON(%s), %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (identifier) DO UPDATE SET
                        alert_category = EXCLUDED.alert_category,
                        feature_type = EXCLUDED.feature_type,
                        geometry = EXCLUDED.geometry,
                        severity = EXCLUDED.severity,
                        effective_start_time = EXCLUDED.effective_start_time,
                        effective_end_time = EXCLUDED.effective_end_time,
                        disaster_type = EXCLUDED.disaster_type,
                        area_description = EXCLUDED.area_description,
                        severity_level = EXCLUDED.severity_level,
                        type = EXCLUDED.type,
                        actual_lang = EXCLUDED.actual_lang,
                        warning_message = EXCLUDED.warning_message,
                        disseminated = EXCLUDED.disseminated,
                        severity_color = EXCLUDED.severity_color,
                        alert_id_sdma_autoinc = EXCLUDED.alert_id_sdma_autoinc,
                        centroid = EXCLUDED.centroid,
                        alert_source = EXCLUDED.alert_source,
                        area_covered = EXCLUDED.area_covered,
                        sender_org_id = EXCLUDED.sender_org_id,
                        polygon_area_covered = EXCLUDED.polygon_area_covered,
                        min_lat = EXCLUDED.min_lat,
                        max_lat = EXCLUDED.max_lat,
                        min_long = EXCLUDED.min_long,
                        max_long = EXCLUDED.max_long,
                        depth = EXCLUDED.depth,
                        intensity = EXCLUDED.intensity,
                        color = EXCLUDED.color,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        radius = EXCLUDED.radius,
                        zone_name = EXCLUDED.zone_name,
                        properties = EXCLUDED.properties
                """, (
                    props.get("identifier"),
                    props.get("alert_category"),
                    props.get("feature_type"),
                    geom_json,
                    props.get("severity"),
                    effective_start,
                    effective_end,
                    props.get("disaster_type"),
                    props.get("area_description"),
                    props.get("severity_level"),
                    props.get("type"),
                    props.get("actual_lang"),
                    props.get("warning_message"),
                    props.get("disseminated"),
                    props.get("severity_color"),
                    props.get("alert_id_sdma_autoinc"),
                    props.get("centroid"),
                    props.get("alert_source"),
                    props.get("area_covered"),
                    props.get("sender_org_id"),
                    props.get("polygon_area_covered"),
                    props.get("min_lat"),
                    props.get("max_lat"),
                    props.get("min_long"),
                    props.get("max_long"),
                    props.get("depth"),
                    props.get("intensity"),
                    props.get("color"),
                    props.get("latitude"),
                    props.get("longitude"),
                    props.get("radius"),
                    props.get("zone_name"),
                    json.dumps(props)
                ))
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting feature with identifier {props.get('identifier')}: {e}")
                skipped_count += 1
                continue
        
        conn.commit()
        print(f"✅ Inserted {inserted_count} features into database")
        if skipped_count > 0:
            print(f"⚠️ Skipped {skipped_count} features due to errors")

def query_sample_data(conn):
    """Query and display sample data to verify insertion"""
    with conn.cursor() as cur:
        # Count total features
        cur.execute("SELECT COUNT(*) FROM cap_alerts")
        total = cur.fetchone()[0]
        print(f"\n📊 Total features in database: {total}")
        
        # Count by feature type
        cur.execute("""
            SELECT feature_type, COUNT(*) 
            FROM cap_alerts 
            GROUP BY feature_type
        """)
        print("\nFeatures by type:")
        for row in cur.fetchall():
            print(f"  • {row[0]}: {row[1]}")
        
        # Count by disaster type
        cur.execute("""
            SELECT disaster_type, COUNT(DISTINCT identifier) 
            FROM cap_alerts 
            GROUP BY disaster_type
            ORDER BY COUNT(DISTINCT identifier) DESC
        """)
        print("\nAlerts by disaster type:")
        for row in cur.fetchall():
            print(f"  • {row[0]}: {row[1]} alert(s)")
        
        # List recent alerts by severity
        cur.execute("""
            SELECT DISTINCT identifier, disaster_type, severity, warning_message, area_description, effective_start_time
            FROM cap_alerts 
            ORDER BY effective_start_time DESC
            LIMIT 10
        """)
        print("\nRecent alerts:")
        for row in cur.fetchall():
            msg_preview = row[3][:60] if row[3] else 'N/A'
            print(f"  • [{row[2]}] {row[1]} - {msg_preview}...")

def store_alerts_to_neondb(geojson_data=None, geojson_path="cap_alerts.geojson", setup_schema=False):
    """
    Store alerts to NeonDB. Can be called with geojson_data directly or load from file.
    
    Args:
        geojson_data: GeoJSON data dictionary (optional, will load from file if not provided)
        geojson_path: Path to GeoJSON file (default: "cap_alerts.geojson")
        setup_schema: Whether to setup/reset the database schema (default: False)
    
    Returns:
        dict: Statistics about the operation
    """
    # Load GeoJSON if not provided
    if geojson_data is None:
        try:
            with open(geojson_path, "r") as f:
                geojson_data = json.load(f)
            print(f"📂 Loaded {len(geojson_data.get('features', []))} features from {geojson_path}")
        except FileNotFoundError:
            print(f"❌ File not found: {geojson_path}")
            return {"success": False, "error": "File not found"}
    
    # Connect to database
    try:
        print("🔌 Connecting to NeonDB...")
        conn = connect_to_db()
        print("✅ Connected to database")
        
        # Setup schema if requested
        if setup_schema:
            print("📋 Setting up database schema...")
            setup_database(conn)
        else:
            # Always ensure new columns exist even without a full schema reset
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

        # Insert features
        print("📥 Storing alerts to NeonDB...")
        features = geojson_data.get("features", [])
        
        if not features:
            print("⚠️ No features to insert")
            conn.close()
            return {"success": True, "inserted": 0, "skipped": 0}
        
        with conn.cursor() as cur:
            inserted_count = 0
            skipped_count = 0
            
            for feature in features:
                props = feature.get("properties", {})
                geometry = feature.get("geometry", {})
                
                # Convert geometry to WKT format for PostGIS (handle null geometry)
                geom_json = json.dumps(geometry) if geometry else None
                
                # Parse timestamps
                effective_start = parse_timestamp(props.get("effective_start_time"))
                effective_end = parse_timestamp(props.get("effective_end_time"))
                
                try:
                    cur.execute("""
                        INSERT INTO cap_alerts (
                            identifier, alert_category, feature_type, geometry, severity, effective_start_time,
                            effective_end_time, disaster_type, area_description, severity_level,
                            type, actual_lang, warning_message, disseminated, severity_color,
                            alert_id_sdma_autoinc, centroid, alert_source, area_covered,
                            sender_org_id, polygon_area_covered, min_lat, max_lat, min_long, max_long,
                            depth, intensity, color, latitude, longitude, radius, zone_name, properties
                        ) VALUES (
                            %s, %s, %s, ST_GeomFromGeoJSON(%s), %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (identifier) DO UPDATE SET
                            alert_category = EXCLUDED.alert_category,
                            feature_type = EXCLUDED.feature_type,
                            geometry = EXCLUDED.geometry,
                            severity = EXCLUDED.severity,
                            effective_start_time = EXCLUDED.effective_start_time,
                            effective_end_time = EXCLUDED.effective_end_time,
                            disaster_type = EXCLUDED.disaster_type,
                            area_description = EXCLUDED.area_description,
                            severity_level = EXCLUDED.severity_level,
                            type = EXCLUDED.type,
                            actual_lang = EXCLUDED.actual_lang,
                            warning_message = EXCLUDED.warning_message,
                            disseminated = EXCLUDED.disseminated,
                            severity_color = EXCLUDED.severity_color,
                            alert_id_sdma_autoinc = EXCLUDED.alert_id_sdma_autoinc,
                            centroid = EXCLUDED.centroid,
                            alert_source = EXCLUDED.alert_source,
                            area_covered = EXCLUDED.area_covered,
                            sender_org_id = EXCLUDED.sender_org_id,
                            polygon_area_covered = EXCLUDED.polygon_area_covered,
                            min_lat = EXCLUDED.min_lat,
                            max_lat = EXCLUDED.max_lat,
                            min_long = EXCLUDED.min_long,
                            max_long = EXCLUDED.max_long,
                            depth = EXCLUDED.depth,
                            intensity = EXCLUDED.intensity,
                            color = EXCLUDED.color,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            radius = EXCLUDED.radius,
                            zone_name = EXCLUDED.zone_name,
                            properties = EXCLUDED.properties
                    """, (
                        props.get("identifier"),
                        props.get("alert_category"),
                        props.get("feature_type"),
                        geom_json,
                        props.get("severity"),
                        effective_start,
                        effective_end,
                        props.get("disaster_type"),
                        props.get("area_description"),
                        props.get("severity_level"),
                        props.get("type"),
                        props.get("actual_lang"),
                        props.get("warning_message"),
                        props.get("disseminated"),
                        props.get("severity_color"),
                        props.get("alert_id_sdma_autoinc"),
                        props.get("centroid"),
                        props.get("alert_source"),
                        props.get("area_covered"),
                        props.get("sender_org_id"),
                        props.get("polygon_area_covered"),
                        props.get("min_lat"),
                        props.get("max_lat"),
                        props.get("min_long"),
                        props.get("max_long"),
                        props.get("depth"),
                        props.get("intensity"),
                        props.get("color"),
                        props.get("latitude"),
                        props.get("longitude"),
                        props.get("radius"),
                        props.get("zone_name"),
                        json.dumps(props)
                    ))
                    inserted_count += 1
                except Exception as e:
                    print(f"⚠️ Error inserting feature with identifier {props.get('identifier')}: {e}")
                    skipped_count += 1
                    continue
            
            conn.commit()
        
        print(f"✅ Stored {inserted_count} alerts to NeonDB")
        if skipped_count > 0:
            print(f"⚠️ Skipped {skipped_count} alerts due to errors")
        
        conn.close()
        return {"success": True, "inserted": inserted_count, "skipped": skipped_count}
        
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nTo set the DATABASE_URL environment variable, add it to your .env file:")
        print("  DATABASE_URL='postgresql://user:password@hostname/database?sslmode=require'")
        return {"success": False, "error": str(e)}
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return {"success": False, "error": str(e)}

def main():
    """Standalone script to store alerts from GeoJSON file to NeonDB"""
    geojson_path = "cap_alerts.geojson"
    
    # Store alerts with schema setup
    result = store_alerts_to_neondb(geojson_path=geojson_path, setup_schema=True)
    
    if result["success"]:
        # Connect to verify data
        try:
            conn = connect_to_db()
            print("\n🔍 Verifying inserted data...")
            query_sample_data(conn)
            conn.close()
            print("\n✅ Done! Data successfully stored in NeonDB")
        except Exception as e:
            print(f"⚠️ Could not verify data: {e}")
    else:
        print(f"\n❌ Failed to store data: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main()
