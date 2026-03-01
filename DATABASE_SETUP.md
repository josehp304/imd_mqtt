# Database Setup Guide

## Setting up Local Docker PostgreSQL

### 1. Start the PostgreSQL Docker Container

Make sure Docker is running, then start the local PostgreSQL container with PostGIS:

```bash
docker run -d \
  --name imd_postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=alertsdb \
  -p 5432:5432 \
  postgis/postgis:15-3.3
```

Or if you already have a `docker-compose.yml`, just run:

```bash
docker compose up -d
```

### 2. Add to .env File

Add the DATABASE_URL to your `.env` file pointing to the local container:

```env
# MQTT Broker Configuration
BROKER_URL=c79937d6d86c41a888807950053c7da9.s1.eu.hivemq.cloud
BROKER_PORT=8883  
USERNAME=josee  
PASSWORD=Josehp123.  

# Local Docker PostgreSQL Configuration
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/alertsdb
```

### 3. Initialize Database Schema

Run this once to create the tables:

```bash
python store_to_neondb.py
```

This will:
- Create the `cap_alerts` table
- Enable PostGIS extension
- Create spatial indexes
- Create indexes on disaster_type, severity, identifier

### 4. Run the Main Program

Now you can run the main program which will:
1. Fetch alerts from NDMA
2. Store them in Neon database
3. Categorize them
4. Publish to MQTT topics

```bash
python main.py
```

## Database Features

### PostGIS Spatial Queries
Once data is stored, you can run spatial queries like:

```sql
-- Find all alerts within a bounding box
SELECT identifier, disaster_type, severity, warning_message
FROM cap_alerts
WHERE geometry IS NOT NULL
AND ST_Intersects(
    geometry,
    ST_MakeEnvelope(77.0, 28.0, 78.0, 29.0, 4326)
);

-- Find alerts by disaster type
SELECT disaster_type, COUNT(*) as count
FROM cap_alerts
GROUP BY disaster_type
ORDER BY count DESC;

-- Find recent severe alerts
SELECT identifier, disaster_type, area_description, effective_start_time
FROM cap_alerts
WHERE severity = 'Severe'
ORDER BY effective_start_time DESC
LIMIT 10;

-- Find alerts near a specific location (within 50km)
SELECT identifier, disaster_type, area_description
FROM cap_alerts
WHERE geometry IS NOT NULL
AND ST_DWithin(
    geography(geometry),
    geography(ST_MakePoint(77.2090, 28.6139)),  -- Delhi coordinates
    50000  -- 50km in meters
);
```

### Table Schema

```sql
CREATE TABLE cap_alerts (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(255) UNIQUE,
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
    alert_source VARCHAR(255),
    properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## How Database Storage Works in main.py

The updated `main.py` now includes automatic database storage:

1. **Fetches alerts** from NDMA API
2. **Stores to Neon database** using `store_alerts_to_neondb()`
   - Uses upsert logic (ON CONFLICT DO UPDATE)
   - Stores geometry data for spatial queries
   - Stores all alert properties
3. **Categorizes alerts** by type
4. **Publishes to MQTT** topics

### Flow Diagram

```
NDMA API → Fetch Alerts
    ↓
Save to GeoJSON/CSV/JSON
    ↓
Store to Local PostgreSQL ← (NEW FEATURE)
    ↓
Categorize Alerts
    ↓
Publish to MQTT Topics
```

## Error Handling

If DATABASE_URL is not set, the system will:
- Show a warning message
- Continue with MQTT publishing
- Not crash or stop the workflow

This allows the system to work even without database configuration.

## Testing Database Connection

To test your database connection:

```bash
python -c "import psycopg2; import os; from dotenv import load_dotenv; load_dotenv(); conn = psycopg2.connect(os.getenv('DATABASE_URL')); print('✅ Database connection successful'); conn.close()"
```

Or connect directly via the Docker container:

```bash
docker exec -it imd_postgres psql -U postgres -d alertsdb
```

## Troubleshooting

### Error: "DATABASE_URL environment variable is not set"
- Add DATABASE_URL to your `.env` file

### Error: "relation 'cap_alerts' does not exist"
- Run `python store_to_neondb.py` to create the schema

### Error: "could not connect to server"
- Make sure the Docker container is running: `docker ps`
- Start it if stopped: `docker start imd_postgres`
- Verify the port mapping is correct (default: 5432)
- Check the connection string matches your container credentials

### Error: "PostGIS extension not available"
- Make sure you're using the `postgis/postgis` Docker image, not plain `postgres`
- Connect to the container and enable it manually:
  ```bash
  docker exec -it imd_postgres psql -U postgres -d alertsdb -c "CREATE EXTENSION IF NOT EXISTS postgis;"
  ```
