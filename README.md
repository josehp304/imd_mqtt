# IMD MQTT Alert System

## Overview
This system fetches disaster alerts from NDMA (National Disaster Management Authority), stores them in a Neon PostgreSQL database, categorizes them by type, and publishes each type to its own MQTT topic.

## Main Components

### 1. `main.py` - Entry Point
The main program that orchestrates the entire workflow:
1. Connects to MQTT broker
2. Fetches alerts from NDMA API
3. **Stores alerts to Neon database** (with spatial data)
4. Categorizes alerts into specific types
5. Publishes each category to its own MQTT topic
6. Listens for incoming messages

### 2. `fetch_alerts.py` - Data Fetcher
Fetches disaster alerts from NDMA CAP API:
- Regular CAP alerts
- Earthquake alerts
- Polygon geometries for alerts
- Exports to JSON, CSV, and GeoJSON formats

### 3. `store_to_neondb.py` - Database Storage
Stores alerts in Neon PostgreSQL database with:
- PostGIS extension for spatial data
- Geometry indexing for efficient location queries
- Full alert properties and metadata
- Upsert logic to handle duplicate alerts
- Indexes on disaster_type, severity, identifier, and spatial geometry

### 4. `alert_categorizer.py` - Alert Categorization
Categorizes disaster alerts into specific types:
- Weather-related (cyclone, rainfall, thunderstorm, etc.)
- Geological (earthquake, tsunami, landslide, avalanche)
- Agricultural/Environmental (drought, pre-fire, pest attack)
- Supports English and Hindi keywords

## Alert Categories & MQTT Topics

### Weather-Related Alerts
- `alerts/weather_cyclone` - Cyclone and cyclonic storms
- `alerts/rainfall_floods` - Rainfall and floods
- `alerts/thunderstorm_lightning` - Thunderstorms and lightning
- `alerts/hailstorm` - Hail storms
- `alerts/cloud_burst` - Cloud bursts
- `alerts/frost_cold_wave` - Frost and cold waves
- `alerts/heat_wave` - Heat waves
- `alerts/dust_storm` - Dust storms

### Geological/Natural Disasters
- `alerts/earthquake` - Earthquakes
- `alerts/tsunami` - Tsunamis
- `alerts/landslide` - Landslides
- `alerts/avalanche` - Avalanches

### Agricultural/Environmental
- `alerts/drought` - Droughts
- `alerts/pre_fire` - Pre-fire and forest fire warnings
- `alerts/pest_attack` - Pest attacks

### Other
- `alerts/other` - Uncategorized alerts

## Configuration

Create a `.env` file with the following variables:

```env
# MQTT Broker Configuration
BROKER_URL=your_mqtt_broker_url
BROKER_PORT=8883
USERNAME=your_mqtt_username
PASSWORD=your_mqtt_password

# Neon Database Configuration
DATABASE_URL=postgresql://user:password@hostname/database?sslmode=require
```

## Database Schema

The `cap_alerts` table includes:
- **Spatial data**: PostGIS geometry for location queries
- **Alert metadata**: identifier, disaster_type, severity, feature_type
- **Temporal data**: effective_start_time, effective_end_time
- **Content**: warning_message, area_description
- **Properties**: Full alert properties as JSONB
- **Indexes**: Spatial index, disaster_type, severity, identifier

### Setup Database Schema
To initialize the database schema:
```bash
python store_to_neondb.py
```

## Installation

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
```

2. Install dependencies:
```bash
pip install paho-mqtt requests python-dotenv psycopg2-binary
```

3. Configure environment variables in `.env`

4. Run the system:
```bash
python main.py
```

## Usage

### Run Main Program
```bash
python main.py
```

### Fetch Alerts Only
```bash
python fetch_alerts.py
```

### Store to Database Only
```bash
python store_to_neondb.py
```

### Test Categorizer
```bash
python alert_categorizer.py
```

## MQTT Message Format

Each MQTT topic receives messages in GeoJSON format:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {...},
      "properties": {
        "identifier": "alert_id",
        "disaster_type": "Earthquake",
        "severity": "Severe",
        "warning_message": "...",
        "area_description": "...",
        "effective_start_time": "...",
        ...
      }
    }
  ],
  "metadata": {
    "alert_type": "earthquake",
    "alert_count": 10,
    "topic": "alerts/earthquake"
  }
}
```

## Example Output

```
Connecting to broker...
✅ Connected successfully!
📡 Subscribed to 14 alert topics

📥 Fetching alerts from NDMA...
Found 40 regular CAP alerts
Found 10 earthquake alerts
Total combined alerts: 50

💾 Storing alerts to NeonDB...
🔌 Connecting to NeonDB...
✅ Connected to database
📥 Storing alerts to NeonDB...
✅ Stored 50 alerts to NeonDB
✅ Database storage complete: 50 alerts stored

📂 Loading alert data...
🔍 Categorizing alerts...

============================================================
📊 ALERT CATEGORIZATION SUMMARY
============================================================
Total Alerts: 50
Categories: 5
------------------------------------------------------------
  • PRE_FIRE             :  30 alerts → Topic: alerts/pre_fire
  • EARTHQUAKE           :  10 alerts → Topic: alerts/earthquake
  • RAINFALL_FLOODS      :   5 alerts → Topic: alerts/rainfall_floods
  • THUNDERSTORM_LIGHTNING :   3 alerts → Topic: alerts/thunderstorm_lightning
  • FROST_COLD_WAVE      :   2 alerts → Topic: alerts/frost_cold_wave
============================================================

📤 Publishing alerts to categorized topics...
  ✅ alerts/pre_fire              : 30 alerts published
  ✅ alerts/earthquake            : 10 alerts published
  ✅ alerts/rainfall_floods       : 5 alerts published
  ✅ alerts/thunderstorm_lightning : 3 alerts published
  ✅ alerts/frost_cold_wave       : 2 alerts published

============================================================
📊 Publishing Summary:
  ✅ Successfully published: 5 topics
  ❌ Failed: 0 topics
  📍 Total alert categories: 5
============================================================
```

## Features

✅ Automatic alert fetching from NDMA API
✅ PostgreSQL/PostGIS database storage with spatial indexing
✅ Intelligent alert categorization (16 types)
✅ Bilingual support (English & Hindi)
✅ Category-specific MQTT topics
✅ GeoJSON format with spatial data
✅ Upsert logic for handling duplicates
✅ Comprehensive error handling
✅ Real-time MQTT publishing

## Dependencies

- `paho-mqtt` - MQTT client library
- `requests` - HTTP API calls
- `python-dotenv` - Environment variable management
- `psycopg2-binary` - PostgreSQL database adapter

## License

MIT License 