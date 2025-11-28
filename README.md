# Distributed Weather System (Team 7)

## Overview
Distributed weather intelligence platform: edge observations flow through MQTT, are stored in TimescaleDB, aggregated and forecasted by FastAPI services, and surfaced via dashboards. Alerts are rule-based and broadcast over MQTT. Architecture and runtime are visualized in an accompanying backend dashboard.

## Team
- Dhruv Kalra (Backend) — Data Stream Handler
- Janaranjanee Sendhil Kumar (Backend) — Alert & Broadcast Module
- Dipal Nirmal (Frontend) — Visualization Dashboard
- Minhajuddin Muhammad (Backend) — Aggregator & Forecast Engine
- Yuvraj Singh Palh (Scrum Master) — Edge Node Data Service

## Architecture
- Edge Nodes: publish JSON observations to MQTT topics `city/{city}/observations`.
- Message Broker: Mosquitto (MQTT TCP + WS) routes observations and alerts.
- Database: TimescaleDB/Postgres (hypertables `observations`, `aggregates`, `alerts`).
- Aggregator & Forecast (FastAPI):
  - MQTT ingestion → Timescale `observations`
  - Aggregation jobs (15m/1h) → `aggregates`
  - Alerts from rules on aggregates → `alerts` table + MQTT `alerts/{city}`
  - OpenWeather forecast with caching; placeholder fallback
  - APIs: `/health`, `/ready`, `/metrics`, `/cities/{id}/observations`, `/aggregates`, `/alerts`, `/forecast`
- Visualization Dashboard: consumes API for observations, aggregates, forecast.
- Backend Dashboard: architecture-style status (edge → broker → DB → aggregator → viz) with live counts.

## Quickstart
1) Prereqs: Docker Desktop; Python 3.11 (for scripts).
2) Start stack:
```sh
docker compose -f infra/docker-compose.yml up --build
```
3) Frontend:  
```sh
cd frontend && python -m http.server 3000
# open http://localhost:3000
# backend architecture dashboard: http://localhost:3000/backend-dashboard.html
```
4) API key: set header `X-API-Key: devkey` (change via env).
5) Seed data (choose one):
- Edge simulator:
```sh
python edge-sim/publisher.py --host localhost --port 1883 --city toronto --source edge-sim --interval 5
```
- Manual publish:
```sh
docker compose -f infra/docker-compose.yml exec mosquitto \
  mosquitto_pub -t city/toronto/observations \
  -m '{"city_id":"toronto","source":"sim","observed_at":"<ISO>","temp_c":36,"humidity":0.58,"wind_kph":45,"pressure_hpa":1010,"rain_mm":0.0}'
```
6) Verify APIs:
```sh
curl -H "X-API-Key: devkey" http://localhost:8080/health
curl -H "X-API-Key: devkey" "http://localhost:8080/cities/toronto/observations?limit=5"
curl -H "X-API-Key: devkey" "http://localhost:8080/cities/toronto/aggregates?window=15%20minutes&limit=5"
curl -H "X-API-Key: devkey" "http://localhost:8080/cities/toronto/alerts?limit=5"
curl -H "X-API-Key: devkey" "http://localhost:8080/cities/toronto/forecast?lat=43.6532&lon=-79.3832"
```

## Data Contracts
- Observation (MQTT/DB): `{city_id, source, observed_at, temp_c, humidity, wind_kph, pressure_hpa, rain_mm}`
- Aggregate (DB): `{city_id, bucket_start, bucket_width, temp_avg, temp_min, temp_max, humidity_avg, wind_avg}`
- Alert (DB/MQTT): `{city_id, level, rule, message, triggered_at}`

## Configuration (env)
- API_KEY (for `X-API-Key`)
- FORECAST_API_KEY / FORECAST_BASE_URL
- MQTT_HOST/PORT/USERNAME/PASSWORD
- AGGREGATE_INTERVAL_SECONDS, ALERT_COOLDOWN_MINUTES
- OPENWEATHER_POLL_SECONDS, OPENWEATHER_CITIES (JSON)
- ALLOWED_ORIGINS

## Key Paths
- Infra: `infra/docker-compose.yml`, `infra/sql/init.sql`, `infra/mosquitto.conf`
- Backend: `aggregator/app/main.py`, `aggregator/app/aggregator.py`, `aggregator/app/alerts.py`, `aggregator/app/forecast.py`, `aggregator/app/openweather_publisher.py`
- Frontend: `frontend/index.html`, `frontend/backend-dashboard.html`, `frontend/style.css`, `frontend/backend-dashboard.css`, `frontend/script.js`, `frontend/backend-dashboard.js`
- Simulator: `edge-sim/publisher.py`
- Demo script: `start.py`

## Highlights
- MQTT decoupled ingestion; edge-friendly.
- Time-series optimized storage (Timescale).
- Rule-based alerts with MQTT broadcast.
- OpenWeather integration with caching.
- Health, readiness, metrics endpoints.
- Demo-friendly: simulator, dashboards, start script.
