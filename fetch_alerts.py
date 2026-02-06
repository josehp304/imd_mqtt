import requests
import json

def fetch_earthquake_alerts():
    """Fetch earthquake alerts from NDMA CAP API"""
    url = "https://sachet.ndma.gov.in/cap_public_website/FetchEarthquakeAlerts"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def convert_to_geojson(alerts_data):
    """Convert earthquake alerts to GeoJSON format"""
    features = []
    
    if not alerts_data or "alerts" not in alerts_data:
        return {"type": "FeatureCollection", "features": []}
    
    for alert in alerts_data["alerts"]:
        # Extract alert properties
        properties = {
            "warning_message": alert.get("warning_message", ""),
            "effective_start_time": alert.get("effective_start_time", ""),
            "depth": alert.get("depth", ""),
        }
        
        # Parse location, magnitude from warning message
        warning_msg = alert.get("warning_message", "")
        if "Magnitude:" in warning_msg:
            try:
                magnitude = warning_msg.split("Magnitude:")[1].split(",")[0].strip()
                properties["magnitude"] = float(magnitude)
            except (IndexError, ValueError):
                pass
        
        if "Lat:" in warning_msg and "Long:" in warning_msg:
            try:
                lat = warning_msg.split("Lat:")[1].split("&")[0].strip()
                lon = warning_msg.split("Long:")[1].split(",")[0].strip()
                properties["latitude"] = float(lat)
                properties["longitude"] = float(lon)
            except (IndexError, ValueError):
                pass
        
        if "Location:" in warning_msg:
            try:
                location = warning_msg.split("Location:")[1].strip()
                properties["location"] = location
            except IndexError:
                pass
        
        # Create point feature for epicenter
        if "latitude" in properties and "longitude" in properties:
            epicenter_feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [properties["longitude"], properties["latitude"]]
                },
                "properties": {
                    **properties,
                    "feature_type": "epicenter"
                }
            }
            features.append(epicenter_feature)
        
        # Create polygon features for intensity zones
        for polygon in alert.get("polygons", []):
            coords = polygon.get("coordinates", {})
            if coords and "coordinates" in coords:
                polygon_feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": coords.get("type", "MultiPolygon"),
                        "coordinates": coords.get("coordinates", [])
                    },
                    "properties": {
                        **properties,
                        "feature_type": "intensity_zone",
                        "intensity": polygon.get("intensity", 0),
                        "color": polygon.get("color", ""),
                        "radius": polygon.get("radius", 0),
                        "zone_name": polygon.get("name", "")
                    }
                }
                features.append(polygon_feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson

def main():
    print("Fetching earthquake alerts from NDMA...")
    alerts_data = fetch_earthquake_alerts()
    
    if alerts_data:
        print(f"Found {len(alerts_data.get('alerts', []))} alerts")
        
        # Convert to GeoJSON
        geojson = convert_to_geojson(alerts_data)
        print(f"Created {len(geojson['features'])} GeoJSON features")
        
        # Save raw response
        with open("earthquake_alerts_raw.json", "w") as f:
            json.dump(alerts_data, f, indent=2)
        print("Saved raw data to earthquake_alerts_raw.json")
        
        # Save GeoJSON
        with open("earthquake_alerts.geojson", "w") as f:
            json.dump(geojson, f, indent=2)
        print("Saved GeoJSON to earthquake_alerts.geojson")
        
        # Print summary
        print("\n--- Summary ---")
        for alert in alerts_data.get("alerts", []):
            msg = alert.get("warning_message", "")
            if "Magnitude:" in msg and "Location:" in msg:
                magnitude = msg.split("Magnitude:")[1].split(",")[0].strip()
                location = msg.split("Location:")[1].strip()
                print(f"  • M{magnitude} - {location}")
    else:
        print("Failed to fetch alerts data")

if __name__ == "__main__":
    main()
