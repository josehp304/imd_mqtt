import requests
import json
import csv
import time

def fetch_all_alerts():
    """Fetch all CAP alerts from NDMA CAP API"""
    url = "https://sachet.ndma.gov.in/cap_public_website/FetchAllAlertDetails"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching alert details: {e}")
        return None

def fetch_earthquake_alerts():
    """Fetch earthquake CAP alerts from NDMA CAP API"""
    url = "https://sachet.ndma.gov.in/cap_public_website/FetchEarthquakeAlerts"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching earthquake alerts: {e}")
        return None

def fetch_alert_polygon(identifier):
    """Fetch polygon data for a specific alert from Geoserver"""
    url = f"https://sachet.ndma.gov.in/cap_public_website/FetchAlertPolygonFromGeoserver?identifier={identifier}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching polygon for {identifier}: {e}")
        return None

def normalize_earthquake_alerts(earthquake_data):
    """Convert earthquake alerts to the same schema as regular CAP alerts"""
    normalized_alerts = []
    
    if not earthquake_data or "alerts" not in earthquake_data:
        return normalized_alerts
    
    for eq_alert in earthquake_data["alerts"]:
        # Create a unique identifier for earthquake alert
        identifier = f"earthquake-{eq_alert.get('effective_start_time', '').replace(' ', '-').replace(':', '-')}"
        
        normalized_alert = {
            "identifier": identifier,
            "severity": "Severe",  # Earthquake alerts are generally severe
            "effective_start_time": eq_alert.get("effective_start_time", ""),
            "effective_end_time": "",  # Not provided in earthquake alerts
            "disaster_type": "Earthquake",
            "area_description": eq_alert.get("warning_message", "").split("Location:")[-1].strip() if "Location:" in eq_alert.get("warning_message", "") else "",
            "severity_level": "",
            "type": "earthquake_cap",
            "actual_lang": "en-IN",
            "warning_message": eq_alert.get("warning_message", ""),
            "disseminated": "",
            "severity_color": "#FF0000",  # Red for earthquakes
            "alert_id_sdma_autoinc": "",
            "centroid": "",
            "alert_source": "IMD",
            "area_covered": "",
            "sender_org_id": "",
            # Earthquake-specific fields
            "depth": eq_alert.get("depth", ""),
            "earthquake_polygons": eq_alert.get("polygons", [])
        }
        normalized_alerts.append(normalized_alert)
    
    return normalized_alerts

def convert_to_geojson(alerts_data, polygons_data):
    """Convert all CAP alerts to GeoJSON format"""
    features = []
    
    if not alerts_data:
        return {"type": "FeatureCollection", "features": []}
    
    for alert in alerts_data:
        # Extract alert identifier
        identifier = alert.get("identifier", "")
        
        # Extract all alert properties from the actual API response
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
            "depth": alert.get("depth", ""),  # For earthquake alerts
        }
        
        # Handle earthquake alerts with built-in polygons
        if alert.get("earthquake_polygons"):
            earthquake_polygons = alert.get("earthquake_polygons", [])
            for poly in earthquake_polygons:
                try:
                    geometry = poly.get("coordinates")
                    polygon_properties = {
                        **properties,
                        "feature_type": "earthquake_zone",
                        "intensity": poly.get("intensity", ""),
                        "color": poly.get("color", ""),
                        "latitude": poly.get("latitude", ""),
                        "longitude": poly.get("longitude", ""),
                        "radius": poly.get("radius", ""),
                        "zone_name": poly.get("zone_name", "")
                    }
                    polygon_feature = {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": polygon_properties
                    }
                    features.append(polygon_feature)
                except (KeyError, TypeError) as e:
                    print(f"  Warning: Failed to parse earthquake polygon for {identifier}: {e}")
            continue
        
        # Get polygon data if we have it
        polygon_data = polygons_data.get(identifier)
        
        if polygon_data and "area_json" in polygon_data:
            # Parse the area_json field which contains the geometry
            try:
                area_json = polygon_data.get("area_json")
                if isinstance(area_json, str):
                    geometry = json.loads(area_json)
                else:
                    geometry = area_json
                
                # Add bounding box info to properties
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
                # Create feature with null geometry on parse error
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
            # No polygon data, create feature with null geometry
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
    print("Fetching all CAP alerts from NDMA...")
    alerts_data = fetch_all_alerts()
    
    if not alerts_data:
        print("Failed to fetch alerts data")
        return
    
    print(f"Found {len(alerts_data)} regular CAP alerts")
    
    # Fetch earthquake alerts
    print("\nFetching earthquake CAP alerts...")
    earthquake_data = fetch_earthquake_alerts()
    
    if earthquake_data:
        earthquake_alerts = normalize_earthquake_alerts(earthquake_data)
        print(f"Found {len(earthquake_alerts)} earthquake alerts")
        
        # Combine earthquake alerts with regular alerts
        alerts_data.extend(earthquake_alerts)
        print(f"Total combined alerts: {len(alerts_data)}")
    else:
        print("No earthquake alerts fetched")
    
    # Fetch polygon data for each regular CAP alert (not earthquake alerts)
    print("\nFetching polygon data for regular CAP alerts...")
    polygons_data = {}
    for i, alert in enumerate(alerts_data):
        # Skip earthquake alerts as they already have polygons
        if alert.get("type") == "earthquake_cap":
            continue
            
        identifier = alert.get("identifier", "")
        if identifier:
            print(f"  Fetching polygon {i+1}/{len(alerts_data)} for {identifier}...")
            polygon = fetch_alert_polygon(identifier)
            if polygon:
                polygons_data[identifier] = polygon
            # Add small delay to avoid overwhelming the server
            time.sleep(0.5)
    
    print(f"Successfully fetched {len(polygons_data)} polygon geometries")
    
    # Convert to GeoJSON
    geojson = convert_to_geojson(alerts_data, polygons_data)
    print(f"Created {len(geojson['features'])} GeoJSON features")
    
    # Save raw response
    with open("cap_alerts_raw.json", "w") as f:
        json.dump(alerts_data, f, indent=2)
    print("Saved raw data to cap_alerts_raw.json")
    
    # Save polygon data
    with open("cap_polygons_raw.json", "w") as f:
        json.dump(polygons_data, f, indent=2)
    print("Saved polygon data to cap_polygons_raw.json")
    
    # Save GeoJSON
    with open("cap_alerts.geojson", "w") as f:
        json.dump(geojson, f, indent=2)
    print("Saved GeoJSON to cap_alerts.geojson")
    
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
        print("Saved CSV to cap_alerts.csv")
    
    # Print summary by disaster type
    print("\n--- Summary by Disaster Type ---")
    disaster_counts = {}
    for alert in alerts_data:
        disaster = alert.get("disaster_type", "Unknown")
        disaster_counts[disaster] = disaster_counts.get(disaster, 0) + 1
    
    for disaster, count in sorted(disaster_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  • {disaster}: {count} alert(s)")

if __name__ == "__main__":
    main()
