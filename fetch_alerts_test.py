"""
Test version of fetch_alerts that includes mock rainfall alerts for testing.
Use this when you need to test sensor-alert matching with rainfall data.
"""
import json
import csv
import time
from datetime import datetime, timedelta

def generate_test_rainfall_alerts():
    """Generate mock rainfall/flood alerts for testing"""
    
    # Current time for alerts
    now = datetime.now()
    start_time = now.strftime("%Y-%m-%d %H:%M:%S")
    end_time = (now + timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
    
    test_alerts = [
        {
            "identifier": "test-rainfall-alert-001",
            "severity": "Severe",
            "effective_start_time": start_time,
            "effective_end_time": end_time,
            "disaster_type": "Rainfall/Floods",
            "area_description": "Delhi and NCR region - Heavy rainfall expected",
            "severity_level": "Red",
            "type": "Alert",
            "actual_lang": "en-IN",
            "warning_message": "Heavy to very heavy rainfall expected in Delhi and surrounding NCR areas. Residents advised to stay indoors and avoid travel unless necessary. Waterlogging expected in low-lying areas.",
            "disseminated": "Public",
            "severity_color": "#FF0000",
            "alert_id_sdma_autoinc": "TEST001",
            "centroid": "28.6139,77.2090",
            "alert_source": "IMD",
            "area_covered": "Delhi NCR",
            "sender_org_id": "IMD",
            "polygon_area_covered": "5000",
            "min_lat": "28.4",
            "max_lat": "28.9",
            "min_long": "76.9",
            "max_long": "77.5"
        },
        {
            "identifier": "test-rainfall-alert-002",
            "severity": "Moderate",
            "effective_start_time": start_time,
            "effective_end_time": end_time,
            "disaster_type": "Rainfall/Floods",
            "area_description": "Mumbai Metropolitan Region - Moderate rainfall with thunderstorms",
            "severity_level": "Orange",
            "type": "Alert",
            "actual_lang": "en-IN",
            "warning_message": "Moderate rainfall with thunderstorms expected in Mumbai and surrounding areas. Possibility of traffic disruptions and minor waterlogging in some areas. Public advised to plan travel accordingly.",
            "disseminated": "Public",
            "severity_color": "#FFA500",
            "alert_id_sdma_autoinc": "TEST002",
            "centroid": "19.0760,72.8777",
            "alert_source": "IMD",
            "area_covered": "Mumbai Metropolitan",
            "sender_org_id": "IMD",
            "polygon_area_covered": "8000",
            "min_lat": "18.9",
            "max_lat": "19.3",
            "min_long": "72.7",
            "max_long": "73.1"
        },
        {
            "identifier": "test-rainfall-alert-003",
            "severity": "Severe",
            "effective_start_time": start_time,
            "effective_end_time": end_time,
            "disaster_type": "Rainfall/Floods",
            "area_description": "Bangalore Urban and Rural - Heavy rainfall warning",
            "severity_level": "Red",
            "type": "Alert",
            "actual_lang": "en-IN",
            "warning_message": "Heavy rainfall expected in Bangalore Urban and Rural districts. Possible flooding in low-lying areas. Residents near lake areas advised to be cautious.",
            "disseminated": "Public",
            "severity_color": "#FF0000",
            "alert_id_sdma_autoinc": "TEST003",
            "centroid": "12.9716,77.5946",
            "alert_source": "IMD",
            "area_covered": "Bangalore",
            "sender_org_id": "IMD",
            "polygon_area_covered": "3500",
            "min_lat": "12.8",
            "max_lat": "13.2",
            "min_long": "77.4",
            "max_long": "77.8"
        },
        {
            "identifier": "test-rainfall-alert-004",
            "severity": "Moderate",
            "effective_start_time": start_time,
            "effective_end_time": end_time,
            "disaster_type": "Rainfall/Floods",
            "area_description": "Kolkata and Howrah - Moderate to heavy rainfall",
            "severity_level": "Orange",
            "type": "Alert",
            "actual_lang": "en-IN",
            "warning_message": "Moderate to heavy rainfall expected in Kolkata and Howrah districts. Waterlogging possible in some areas. Public advised to avoid unnecessary travel.",
            "disseminated": "Public",
            "severity_color": "#FFA500",
            "alert_id_sdma_autoinc": "TEST004",
            "centroid": "22.5726,88.3639",
            "alert_source": "IMD",
            "area_covered": "Kolkata Metro",
            "sender_org_id": "IMD",
            "polygon_area_covered": "4000",
            "min_lat": "22.4",
            "max_lat": "22.7",
            "min_long": "88.2",
            "max_long": "88.5"
        },
        {
            "identifier": "test-rainfall-alert-005",
            "severity": "Severe",
            "effective_start_time": start_time,
            "effective_end_time": end_time,
            "disaster_type": "Rainfall/Floods",
            "area_description": "Madhya Pradesh - Jabalpur region heavy rainfall",
            "severity_level": "Red",
            "type": "Alert",
            "actual_lang": "en-IN",
            "warning_message": "Very heavy rainfall expected in Jabalpur and surrounding areas of Madhya Pradesh. Flash floods possible in hilly areas. Residents advised to move to safer locations.",
            "disseminated": "Public",
            "severity_color": "#FF0000",
            "alert_id_sdma_autoinc": "TEST005",
            "centroid": "21.26,77.41",
            "alert_source": "IMD",
            "area_covered": "Jabalpur District",
            "sender_org_id": "IMD",
            "polygon_area_covered": "6000",
            "min_lat": "21.0",
            "max_lat": "21.5",
            "min_long": "77.2",
            "max_long": "77.6"
        }
    ]
    
    return test_alerts

def generate_test_polygons():
    """Generate polygon geometries for test rainfall alerts"""
    
    polygons = {
        "test-rainfall-alert-001": {
            "identifier": "test-rainfall-alert-001",
            "area_covered": "Delhi NCR",
            "min_lat": "28.4",
            "max_lat": "28.9",
            "min_long": "76.9",
            "max_long": "77.5",
            "area_json": {
                "type": "Polygon",
                "coordinates": [[
                    [76.9, 28.4],
                    [77.5, 28.4],
                    [77.5, 28.9],
                    [76.9, 28.9],
                    [76.9, 28.4]
                ]]
            }
        },
        "test-rainfall-alert-002": {
            "identifier": "test-rainfall-alert-002",
            "area_covered": "Mumbai Metropolitan",
            "min_lat": "18.9",
            "max_lat": "19.3",
            "min_long": "72.7",
            "max_long": "73.1",
            "area_json": {
                "type": "Polygon",
                "coordinates": [[
                    [72.7, 18.9],
                    [73.1, 18.9],
                    [73.1, 19.3],
                    [72.7, 19.3],
                    [72.7, 18.9]
                ]]
            }
        },
        "test-rainfall-alert-003": {
            "identifier": "test-rainfall-alert-003",
            "area_covered": "Bangalore",
            "min_lat": "12.8",
            "max_lat": "13.2",
            "min_long": "77.4",
            "max_long": "77.8",
            "area_json": {
                "type": "Polygon",
                "coordinates": [[
                    [77.4, 12.8],
                    [77.8, 12.8],
                    [77.8, 13.2],
                    [77.4, 13.2],
                    [77.4, 12.8]
                ]]
            }
        },
        "test-rainfall-alert-004": {
            "identifier": "test-rainfall-alert-004",
            "area_covered": "Kolkata Metro",
            "min_lat": "22.4",
            "max_lat": "22.7",
            "min_long": "88.2",
            "max_long": "88.5",
            "area_json": {
                "type": "Polygon",
                "coordinates": [[
                    [88.2, 22.4],
                    [88.5, 22.4],
                    [88.5, 22.7],
                    [88.2, 22.7],
                    [88.2, 22.4]
                ]]
            }
        },
        "test-rainfall-alert-005": {
            "identifier": "test-rainfall-alert-005",
            "area_covered": "Jabalpur District",
            "min_lat": "21.0",
            "max_lat": "21.5",
            "min_long": "77.2",
            "max_long": "77.6",
            "area_json": {
                "type": "Polygon",
                "coordinates": [[
                    [77.2, 21.0],
                    [77.6, 21.0],
                    [77.6, 21.5],
                    [77.2, 21.5],
                    [77.2, 21.0]
                ]]
            }
        }
    }
    
    return polygons

def convert_to_geojson(alerts_data, polygons_data):
    """Convert test CAP alerts to GeoJSON format"""
    features = []
    
    if not alerts_data:
        return {"type": "FeatureCollection", "features": []}
    
    for alert in alerts_data:
        identifier = alert.get("identifier", "")
        
        properties = {
            "identifier": identifier,
            "severity": alert.get("severity", ""),
            "effective_start_time": alert.get("effective_start_time", ""),
            "effective_end_time": alert.get("effective_end_time", ""),
            "disaster_type": alert.get("disaster_type", ""),
            "area_description": alert.get("area_description", ""),
            "severity_level": alert.get("severity_level", ""),
            "type": alert.get("type", ""),
            "actual_lang": alert.get("actual_lang", ""),
            "warning_message": alert.get("warning_message", ""),
            "disseminated": alert.get("disseminated", ""),
            "severity_color": alert.get("severity_color", ""),
            "alert_id_sdma_autoinc": alert.get("alert_id_sdma_autoinc", ""),
            "centroid": alert.get("centroid", ""),
            "alert_source": alert.get("alert_source", ""),
            "area_covered": alert.get("area_covered", ""),
            "sender_org_id": alert.get("sender_org_id", ""),
        }
        
        polygon_data = polygons_data.get(identifier)
        
        if polygon_data and "area_json" in polygon_data:
            try:
                area_json = polygon_data.get("area_json")
                if isinstance(area_json, str):
                    geometry = json.loads(area_json)
                else:
                    geometry = area_json
                
                polygon_properties = {
                    **properties,
                    "feature_type": "alert_area",
                    "polygon_area_covered": polygon_data.get("area_covered", ""),
                    "min_lat": polygon_data.get("min_lat", ""),
                    "max_lat": polygon_data.get("max_lat", ""),
                    "min_long": polygon_data.get("min_long", ""),
                    "max_long": polygon_data.get("max_long", "")
                }
                
                polygon_feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": polygon_properties
                }
                features.append(polygon_feature)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"  Warning: Failed to parse area_json for {identifier}: {e}")
                feature = {
                    "type": "Feature",
                    "geometry": None,
                    "properties": {
                        **properties,
                        "feature_type": "alert_no_geometry"
                    }
                }
                features.append(feature)
        else:
            feature = {
                "type": "Feature",
                "geometry": None,
                "properties": {
                    **properties,
                    "feature_type": "alert_no_geometry"
                }
            }
            features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson

def convert_to_csv(alerts_data):
    """Convert CAP alerts to CSV format"""
    rows = []
    
    if not alerts_data:
        return rows
    
    for alert in alerts_data:
        row = {
            "identifier": alert.get("identifier", ""),
            "severity": alert.get("severity", ""),
            "effective_start_time": alert.get("effective_start_time", ""),
            "effective_end_time": alert.get("effective_end_time", ""),
            "disaster_type": alert.get("disaster_type", ""),
            "area_description": alert.get("area_description", ""),
            "severity_level": alert.get("severity_level", ""),
            "warning_message": alert.get("warning_message", ""),
            "alert_source": alert.get("alert_source", ""),
            "area_covered": alert.get("area_covered", ""),
        }
        rows.append(row)
    
    return rows

def main():
    print("=" * 70)
    print("🧪 GENERATING TEST RAINFALL ALERTS")
    print("=" * 70)
    print("\n⚠️  NOTE: This is TEST DATA for development purposes")
    print("   Use fetch_alerts.py for real NDMA data\n")
    
    # Generate test rainfall alerts
    print("Generating test rainfall alerts...")
    alerts_data = generate_test_rainfall_alerts()
    print(f"✅ Generated {len(alerts_data)} test rainfall alerts")
    
    # Generate test polygons
    print("\nGenerating test polygon geometries...")
    polygons_data = generate_test_polygons()
    print(f"✅ Generated {len(polygons_data)} test polygon geometries")
    
    # Convert to GeoJSON
    print("\nConverting to GeoJSON...")
    geojson = convert_to_geojson(alerts_data, polygons_data)
    print(f"✅ Created {len(geojson['features'])} GeoJSON features")
    
    # Save raw response
    with open("cap_alerts_raw.json", "w") as f:
        json.dump(alerts_data, f, indent=2)
    print("\n📁 Saved raw data to cap_alerts_raw.json")
    
    # Save polygon data
    with open("cap_polygons_raw.json", "w") as f:
        json.dump(polygons_data, f, indent=2)
    print("📁 Saved polygon data to cap_polygons_raw.json")
    
    # Save GeoJSON
    with open("cap_alerts.geojson", "w") as f:
        json.dump(geojson, f, indent=2)
    print("📁 Saved GeoJSON to cap_alerts.geojson")
    
    # Convert to CSV
    csv_data = convert_to_csv(alerts_data)
    
    # Save CSV
    if csv_data:
        with open("cap_alerts.csv", "w", newline='', encoding='utf-8') as f:
            fieldnames = ["identifier", "severity", "effective_start_time", "effective_end_time", 
                         "disaster_type", "area_description", "severity_level", "warning_message",
                         "alert_source", "area_covered"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
        print("📁 Saved CSV to cap_alerts.csv")
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 TEST ALERT SUMMARY")
    print("=" * 70)
    
    for alert in alerts_data:
        print(f"\n🌧️  {alert['identifier']}")
        print(f"   Location: {alert['area_description']}")
        print(f"   Severity: {alert['severity']} ({alert['severity_level']})")
        print(f"   Coverage: {alert['min_lat']}-{alert['max_lat']}°N, {alert['min_long']}-{alert['max_long']}°E")
    
    print("\n" + "=" * 70)
    print("✅ Test data generation complete!")
    print("=" * 70)
    print("\n💡 Next steps:")
    print("   1. Run: python sensor_status_listener.py")
    print("   2. Run: python test_sensor_publisher.py")
    print("   3. Run: python main.py  (will use these test alerts)")
    print("\n")

if __name__ == "__main__":
    main()
