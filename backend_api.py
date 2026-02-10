from flask import Flask, jsonify, send_file
from flask_cors import CORS
import psycopg2
import json
import os

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://neondb_owner:npg_Nxr0ZovhA7dG@ep-broad-bar-a1abido7-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(DATABASE_URL)

@app.route('/')
def index():
    """Serve the frontend HTML"""
    return send_file('index.html')

@app.route('/api/alerts/latest', methods=['GET'])
def get_latest_alerts():
    """Get all latest CAP alerts as GeoJSON"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all features from cap_alerts
        cur.execute("""
            SELECT 
                identifier,
                feature_type,
                ST_AsGeoJSON(geometry) as geometry,
                severity,
                effective_start_time,
                disaster_type,
                area_description,
                warning_message,
                severity_color,
                depth,
                intensity,
                color,
                latitude,
                longitude,
                radius,
                zone_name,
                properties
            FROM cap_alerts
            ORDER BY created_at DESC
        """)
        
        features = []
        for row in cur.fetchall():
            feature = {
                "type": "Feature",
                "geometry": json.loads(row[2]) if row[2] else None,
                "properties": {
                    "identifier": row[0],
                    "feature_type": row[1],
                    "severity": row[3],
                    "effective_start_time": row[4].isoformat() if row[4] else None,
                    "disaster_type": row[5],
                    "area_description": row[6],
                    "warning_message": row[7],
                    "severity_color": row[8],
                    "depth": row[9],
                    "intensity": float(row[10]) if row[10] else None,
                    "color": row[11],
                    "latitude": float(row[12]) if row[12] else None,
                    "longitude": float(row[13]) if row[13] else None,
                    "radius": float(row[14]) if row[14] else None,
                    "zone_name": row[15]
                }
            }
            features.append(feature)
        
        cur.close()
        conn.close()
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/alerts/epicenters', methods=['GET'])
def get_epicenters():
    """Get only earthquake epicenter points"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                ST_AsGeoJSON(geometry) as geometry,
                area_description,
                latitude,
                longitude,
                depth,
                effective_start_time,
                warning_message
            FROM cap_alerts
            WHERE disaster_type = 'Earthquake' AND latitude IS NOT NULL
            ORDER BY effective_start_time DESC
        """)
        
        features = []
        for row in cur.fetchall():
            feature = {
                "type": "Feature",
                "geometry": json.loads(row[0]) if row[0] else None,
                "properties": {
                    "location": row[1],
                    "latitude": float(row[2]) if row[2] else None,
                    "longitude": float(row[3]) if row[3] else None,
                    "depth": row[4],
                    "effective_start_time": row[5].isoformat() if row[5] else None,
                    "warning_message": row[6]
                }
            }
            features.append(feature)
        
        cur.close()
        conn.close()
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        return jsonify(geojson)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
