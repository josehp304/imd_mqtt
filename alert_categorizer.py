"""
Alert Categorizer Module
Categorizes disaster alerts into specific alert types for MQTT topic routing
"""

def categorize_alert(disaster_type, warning_message=""):
    """
    Categorize alerts into specific types based on disaster_type and warning_message
    
    Alert Categories:
    - weather_cyclone
    - rainfall_floods
    - thunderstorm_lightning
    - hailstorm
    - cloud_burst
    - frost_cold_wave
    - earthquake
    - tsunami
    - landslide
    - avalanche
    - drought
    - pre_fire
    - pest_attack
    - other (for unmatched alerts)
    
    Args:
        disaster_type (str): The disaster type from the alert
        warning_message (str): The warning message from the alert
    
    Returns:
        str: The categorized alert type (topic name)
    """
    
    if not disaster_type:
        return "other"
    
    # Convert to lowercase for case-insensitive matching
    disaster_lower = disaster_type.lower()
    message_lower = warning_message.lower() if warning_message else ""
    
    # Earthquake
    if "earthquake" in disaster_lower or "भूकंप" in message_lower:
        return "earthquake"
    
    # Tsunami
    if "tsunami" in disaster_lower or "सुनामी" in message_lower:
        return "tsunami"
    
    # Landslide
    if "landslide" in disaster_lower or "land slide" in disaster_lower or "भूस्खलन" in message_lower:
        return "landslide"
    
    # Avalanche
    if "avalanche" in disaster_lower or "हिमस्खलन" in message_lower:
        return "avalanche"
    
    # Weather Cyclone
    if "cyclone" in disaster_lower or "cyclonic" in disaster_lower or "चक्रवात" in message_lower:
        return "weather_cyclone"
    
    # Rainfall/Floods
    if any(keyword in disaster_lower for keyword in ["rainfall", "rain", "flood", "heavy rain", "extremely heavy rain"]) or \
       any(keyword in message_lower for keyword in ["बाढ़", "बारिश", "वर्षा"]):
        return "rainfall_floods"
    
    # Thunderstorm/Lightning
    if any(keyword in disaster_lower for keyword in ["thunderstorm", "thunder storm", "lightning", "thunder"]) or \
       any(keyword in message_lower for keyword in ["आंधी", "तड़ित", "बिजली", "गरज"]):
        return "thunderstorm_lightning"
    
    # Hailstorm
    if "hail" in disaster_lower or "ओला" in message_lower or "ओलावृष्टि" in message_lower:
        return "hailstorm"
    
    # Cloud Burst
    if "cloudburst" in disaster_lower or "cloud burst" in disaster_lower or "बादल फटना" in message_lower:
        return "cloud_burst"
    
    # Frost/Cold Wave
    if any(keyword in disaster_lower for keyword in ["frost", "cold wave", "coldwave", "cold", "freeze"]) or \
       any(keyword in message_lower for keyword in ["शीत लहर", "पाला", "ठंड"]):
        return "frost_cold_wave"
    
    # Drought
    if "drought" in disaster_lower or "सूखा" in message_lower:
        return "drought"
    
    # Pre-Fire / Forest Fire
    if any(keyword in disaster_lower for keyword in ["pre fire", "pre-fire", "fire", "forest fire"]) or \
       any(keyword in message_lower for keyword in ["जंगल में आग", "आग", "forest fire"]):
        return "pre_fire"
    
    # Pest Attack
    if "pest" in disaster_lower or "कीट" in message_lower:
        return "pest_attack"
    
    # Heat Wave (additional category that might be useful)
    if any(keyword in disaster_lower for keyword in ["heat", "hot"]) or "गर्मी की लहर" in message_lower:
        return "heat_wave"
    
    # Dust Storm (additional category)
    if "dust" in disaster_lower or "धूल" in message_lower:
        return "dust_storm"
    
    # Default to other for unmatched types
    return "other"


def get_topic_name(alert_type, prefix="alerts"):
    """
    Generate MQTT topic name for the alert type
    
    Args:
        alert_type (str): The categorized alert type
        prefix (str): The topic prefix (default: "alerts")
    
    Returns:
        str: The full MQTT topic name
    """
    return f"{prefix}/{alert_type}"


def categorize_alerts_batch(alerts_data):
    """
    Categorize a batch of alerts and group them by type
    
    Args:
        alerts_data (list): List of alert dictionaries
    
    Returns:
        dict: Dictionary mapping alert types to lists of alerts
    """
    categorized = {}
    
    for alert in alerts_data:
        disaster_type = alert.get("disaster_type", "")
        warning_message = alert.get("warning_message", "")
        
        alert_type = categorize_alert(disaster_type, warning_message)
        
        if alert_type not in categorized:
            categorized[alert_type] = []
        
        # Add the alert type to the alert data for reference
        alert_with_type = alert.copy()
        alert_with_type["alert_category"] = alert_type
        
        categorized[alert_type].append(alert_with_type)
    
    return categorized


def print_categorization_summary(categorized_alerts):
    """
    Print a summary of categorized alerts
    
    Args:
        categorized_alerts (dict): Dictionary mapping alert types to lists of alerts
    """
    print("\n" + "="*60)
    print("📊 ALERT CATEGORIZATION SUMMARY")
    print("="*60)
    
    total_alerts = sum(len(alerts) for alerts in categorized_alerts.values())
    print(f"Total Alerts: {total_alerts}")
    print(f"Categories: {len(categorized_alerts)}")
    print("-"*60)
    
    for alert_type, alerts in sorted(categorized_alerts.items(), key=lambda x: len(x[1]), reverse=True):
        count = len(alerts)
        topic = get_topic_name(alert_type)
        print(f"  • {alert_type.upper():<20} : {count:>3} alerts → Topic: {topic}")
    
    print("="*60 + "\n")


if __name__ == "__main__":
    # Test the categorizer
    test_alerts = [
        {"disaster_type": "Pre Fire", "warning_message": "Forest fire risk"},
        {"disaster_type": "Earthquake", "warning_message": "Earthquake detected"},
        {"disaster_type": "Heavy Rainfall", "warning_message": "Heavy rain expected"},
        {"disaster_type": "Thunderstorm", "warning_message": "Thunderstorm warning"},
        {"disaster_type": "Cold Wave", "warning_message": "Cold wave conditions"},
    ]
    
    print("Testing Alert Categorizer...")
    for alert in test_alerts:
        category = categorize_alert(alert["disaster_type"], alert["warning_message"])
        print(f"{alert['disaster_type']} → {category}")
    
    print("\nBatch categorization test:")
    categorized = categorize_alerts_batch(test_alerts)
    print_categorization_summary(categorized)
