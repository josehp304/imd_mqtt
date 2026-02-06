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
    """Get the latest earthquake alerts as GeoJSON"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all features from the latest batch (by created_at)
        cur.execute("""
            SELECT 
                feature_type,
                ST_AsGeoJSON(geometry) as geometry,
                warning_message,
                effective_start_time,
                depth,
                magnitude,
                latitude,
                longitude,
                location,
                intensity,
                color,
                radius,
                zone_name,
                properties
            FROM earthquake_alerts
            ORDER BY created_at DESC
        """)
        
        features = []
        for row in cur.fetchall():
            feature = {
                "type": "Feature",
                "geometry": json.loads(row[1]) if row[1] else None,
                "properties": {
                    "feature_type": row[0],
                    "warning_message": row[2],
                    "effective_start_time": row[3].isoformat() if row[3] else None,
                    "depth": row[4],
                    "magnitude": float(row[5]) if row[5] else None,
                    "latitude": float(row[6]) if row[6] else None,
                    "longitude": float(row[7]) if row[7] else None,
                    "location": row[8],
                    "intensity": float(row[9]) if row[9] else None,
                    "color": row[10],
                    "radius": float(row[11]) if row[11] else None,
                    "zone_name": row[12]
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
    """Get only the epicenter points"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                ST_AsGeoJSON(geometry) as geometry,
                location,
                magnitude,
                latitude,
                longitude,
                depth,
                effective_start_time
            FROM earthquake_alerts
            WHERE feature_type = 'epicenter'
            ORDER BY magnitude DESC
        """)
        
        features = []
        for row in cur.fetchall():
            feature = {
                "type": "Feature",
                "geometry": json.loads(row[0]) if row[0] else None,
                "properties": {
                    "location": row[1],
                    "magnitude": float(row[2]) if row[2] else None,
                    "latitude": float(row[3]) if row[3] else None,
                    "longitude": float(row[4]) if row[4] else None,
                    "depth": row[5],
                    "effective_start_time": row[6].isoformat() if row[6] else None
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
