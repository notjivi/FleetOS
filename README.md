# FleetOS

**Real-time fleet intelligence platform.** Ingests telemetry from 1,000 simulated vehicles, detects anomalies, enforces geofences, tracks trips, and surfaces everything on a live dashboard — all running locally via a single Docker Compose command.

> I got curious about how companies like Uber Freight and fleet telematics platforms actually work under the hood — how do you handle a thousand vehicles streaming GPS data simultaneously without the database falling over? This is my attempt to build that from scratch and understand every layer. Every architectural decision is documented and defensible. This is not a tutorial follow-along — it solves real distributed systems problems.

---

## Live Demo

```
docker-compose up
```

Open `http://localhost:3000`. Watch 1,000 vehicles move across a map in real time.

---

## What It Does

| Subsystem | What happens |
|---|---|
| **Ingestion** | 1,000 vehicle simulators emit GPS + telemetry over WebSockets every 2 seconds |
| **Queue** | RabbitMQ absorbs bursts — API never touches the database directly |
| **Processing** | Worker pool runs anomaly detection, geofencing, and trip state on every packet |
| **Storage** | TimescaleDB stores all telemetry in a time-partitioned hypertable |
| **State** | Redis holds current vehicle position for sub-millisecond dashboard reads |
| **API** | FastAPI REST + WebSocket layer with JWT auth and per-endpoint rate limiting |
| **Dashboard** | React + Leaflet Canvas renders 1,000 live vehicle positions without freezing the browser |
| **Reaper** | Background process closes abandoned trips from vehicles that lost connectivity mid-trip |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        VEHICLE SIMULATOR                         │
│         1,000 asyncio coroutines — one per vehicle              │
│         Emits JSON over WebSocket every 2 seconds               │
└───────────────────────────┬─────────────────────────────────────┘
                            │ WebSocket
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FASTAPI GATEWAY                           │
│         Pydantic validation → publish to RabbitMQ               │
│         Returns immediately. Never blocks on DB.                │
│         3 replicas running in parallel                          │
└──────────────┬────────────────────────────────┬─────────────────┘
               │ AMQP publish                   │ WebSocket push
               ▼                                ▼
┌──────────────────────┐             ┌──────────────────────────┐
│      RABBITMQ        │             │    CONNECTED DASHBOARDS  │
│                      │             │    (Redis Pub/Sub relay)  │
│  telemetry exchange  │             └──────────────────────────┘
│  alerts exchange     │
└──────────┬───────────┘
           │ consume (batch 50-100)
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     WORKER POOL (4 processes)                    │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ Anomaly         │  │ Geofencing   │  │ Trip State        │  │
│  │ Detection       │  │ Engine       │  │ Machine           │  │
│  │                 │  │              │  │                   │  │
│  │ Rolling window  │  │ Haversine    │  │ SETNX lock        │  │
│  │ LRU-bounded     │  │ NumPy batch  │  │ IDLE→ACTIVE       │  │
│  │ per vehicle     │  │ Redis SET    │  │ ACTIVE→COMPLETED  │  │
│  └─────────────────┘  └──────────────┘  └───────────────────┘  │
│                                                                  │
│  Bulk INSERT → TimescaleDB        Redis HSET → vehicle state    │
└─────────────────────────────────────────────────────────────────┘
           │                                    │
           ▼                                    ▼
┌─────────────────────┐             ┌──────────────────────────┐
│    TIMESCALEDB      │             │         REDIS            │
│                     │             │                          │
│  vehicle_telemetry  │             │  vehicle:state:{id}      │
│  trips              │             │  vehicle:trip:{id}       │
│  anomalies          │             │  vehicle:geofences:{id}  │
│  geofence_events    │             │  rate_limit:{ip}:{ep}    │
│  vehicles           │             │                          │
│  geofences          │             │  TTL: 30s on state keys  │
└─────────────────────┘             └──────────────────────────┘
                                                │
                              ┌─────────────────┘
                              │ every 5 min
                              ▼
                  ┌───────────────────────┐
                  │       REAPER          │
                  │                       │
                  │  Checks Redis TTL     │
                  │  Closes ghost trips   │
                  │  status=ABANDONED     │
                  └───────────────────────┘
```

---

## The Hard Problems This Project Solves

These are not textbook problems. They surfaced during development and required real solutions.

### 1. Concurrent Trip Creation Race Condition

**The bug:** Two workers process consecutive packets from the same vehicle simultaneously. Both read `IDLE` from Redis. Both create a new trip record. Trip count doubles.

**The fix:** Redis distributed lock using `SETNX` with a 5-second TTL. Worker acquires lock keyed to `vehicle:lock:{id}` before any trip state transition. If lock exists, skip packet — the holding worker processes it within milliseconds.

**The known limitation:** If a worker experiences a GC pause longer than the TTL, the lock expires and a second worker can acquire it. The first worker wakes up believing it still holds the lock. Production fix is fencing tokens — the lock acquisition returns a monotonically increasing token, every database write conditions on that token, so late writers are rejected at the database level. For this scale, SETNX with TTL is a justified approximation.

```python
lock_key = f"vehicle:lock:{vehicle_id}"
acquired = redis.set(lock_key, "1", nx=True, ex=5)
if not acquired:
    return  # another worker has this vehicle, skip
try:
    process_trip_state(vehicle_id, packet)
finally:
    redis.delete(lock_key)
```

---

### 2. Ghost Vehicles (The Reaper Problem)

**The bug:** A vehicle loses network connectivity mid-trip. It never sends `engine_on: false`. The trip record stays `ACTIVE` in the database forever with `ended_at = NULL`. Over months, thousands of zombie trips accumulate and corrupt fleet analytics.

**The fix:** The Reaper process runs every 5 minutes. It queries the vehicles table, checks Redis for each vehicle's state key. If the key has expired (vehicle silent for 30+ seconds) and there's an `ACTIVE` trip older than 10 minutes, the Reaper closes it:

```python
# Reaper logic
for vehicle_id in all_vehicle_ids:
    if not redis.exists(f"vehicle:state:{vehicle_id}"):
        close_abandoned_trip(vehicle_id)

def close_abandoned_trip(vehicle_id):
    last_packet = get_last_telemetry(vehicle_id)  # TimescaleDB query
    db.execute("""
        UPDATE trips
        SET status = 'ABANDONED',
            ended_at = %s,
            distance_km = compute_distance(trip_id)
        WHERE vehicle_id = %s AND status = 'ACTIVE'
        AND started_at < NOW() - INTERVAL '10 minutes'
    """, [last_packet.time, vehicle_id])
```

**Why Redis TTL as the liveness signal instead of querying TimescaleDB directly:** 1,000 vehicles × every 5 minutes = 1,000 database queries per Reaper cycle on the primary write path. Redis key existence is O(1) with no disk I/O. The 30-second TTL already tracks liveness — reusing it costs nothing.

**Why a `status` field instead of just checking `ended_at IS NOT NULL`:** Without a status field, you cannot distinguish a currently active trip from a ghost trip. `ended_at IS NOT NULL` catches completed trips but misses ongoing ones. `status = 'COMPLETED'` vs `status = 'ABANDONED'` makes the distinction explicit and queryable — analytics queries use `WHERE status = 'COMPLETED'` to exclude incomplete data cleanly.

---

### 3. Out-of-Order Packets (The Offline Dump Problem)

**The bug:** A vehicle drives through a dead zone, buffers 500 packets locally, reconnects, and dumps them all at once. The worker processes these historical packets alongside real-time packets. Rolling averages break. Speed spike detection fires false positives. Trip state machine sees `engine_off` before `engine_on`.

**The fix:** Split ingestion paths at the gateway. Historical packets (timestamp > 60 seconds behind wall clock) route to `telemetry.historical` queue. Real-time packets route to `telemetry.realtime`. Historical workers sort by timestamp per `vehicle_id` before processing and use relaxed anomaly thresholds. Anomalies detected in historical data are written with `is_historical = true` and filtered from the live alert feed.

```python
# Gateway routing decision
age_seconds = (datetime.utcnow() - packet.timestamp).total_seconds()
queue = "telemetry.historical" if age_seconds > 60 else "telemetry.realtime"
channel.basic_publish(exchange="telemetry", routing_key=queue, body=payload)
```

---

### 4. RabbitMQ Cannot Guarantee Per-Vehicle Message Ordering

**The constraint:** With 4 worker processes consuming from the same queue, Worker A may pull packet `T=1` and Worker B may pull packet `T=2`. If Worker A stalls (GC pause, slow DB write), Worker B writes `T=2` first. The trip state machine receives `engine_off` before `engine_on`.

**The mitigation:** Before any Redis state update, compare the incoming packet's timestamp against the current value in `vehicle:state:{id}`. If `current_timestamp > packet_timestamp`, discard — this packet is stale and already superseded.

```python
current = redis.hget(f"vehicle:state:{vehicle_id}", "last_seen")
if current and float(current) > packet.timestamp.timestamp():
    return  # stale packet, discard
```

**The honest limitation:** This prevents the worst outcomes but does not guarantee strict ordering. For true per-vehicle ordering guarantees, Kafka with `vehicle_id` as the partition key is the correct solution — all packets for a given vehicle go to the same partition, consumed by the same worker, in sequence. RabbitMQ was chosen here for its simplicity at this scale. The architecture is Kafka-ready — swapping the broker changes no application code.

---

### 5. Worker Memory Leak (Unbounded In-Process State)

**The bug:** The anomaly detector stores a rolling deque of 10 packets per vehicle in worker memory. A vehicle sends one packet and goes permanently offline. Its deque is never freed. Over months, the worker accumulates state for tens of thousands of vehicles that no longer exist.

**The fix:** Replace the raw dict with an LRU cache. When capacity is exceeded, the least recently accessed vehicle's state is evicted automatically.

```python
from cachetools import LRUCache

# Bounded at 2x expected active vehicles
vehicle_windows: LRUCache = LRUCache(maxsize=2000)
```

Cost: zero. One import, one type annotation change. A vehicle that goes offline is evicted when 2,000 other vehicles become active — its deque is garbage collected. If it reconnects, a fresh deque is created.

---

### 6. Browser Rendering Collapse at 1,000 Animated Markers

**The bug:** Standard Leaflet renders each vehicle as a DOM element — an HTML `<div>`. 1,000 divs updating position every 2 seconds means 1,000 DOM mutations per tick, each triggering browser layout recalculation. The main thread saturates. CPU hits 100%. The tab freezes within seconds.

**The fix:** Leaflet's Canvas renderer draws all markers onto a single `<canvas>` element. One composite paint per tick instead of 1,000 layout recalculations. Additionally, marker positions are updated directly on the Leaflet layer — never through React state, which would trigger reconciliation on 1,000 simultaneous updates.

```javascript
// Wrong — 1,000 DOM elements
const marker = L.marker([lat, lng]);

// Right — all markers on one canvas
const marker = L.circleMarker([lat, lng], {
  renderer: L.canvas(),
  radius: 4
});

// Update positions directly, never via React setState
markerRef.current.setLatLng([newLat, newLng]);
```

Render CPU: 100% → under 15%. Verified in Chrome DevTools Performance tab.

---

## Database Schema

```sql
-- Primary time-series store
-- Hypertable auto-partitions by time (7-day chunks)
CREATE TABLE vehicle_telemetry (
    time          TIMESTAMPTZ      NOT NULL,
    vehicle_id    VARCHAR(20)      NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    speed         SMALLINT         NOT NULL,
    fuel          SMALLINT         NOT NULL,
    engine_on     BOOLEAN          NOT NULL,
    PRIMARY KEY (vehicle_id, time)  -- composite index: vehicle-first for range queries
);
SELECT create_hypertable('vehicle_telemetry', 'time');

-- Trip lifecycle with explicit status for analytics filtering
CREATE TABLE trips (
    trip_id     UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_id  VARCHAR(20)      NOT NULL,
    started_at  TIMESTAMPTZ      NOT NULL,
    ended_at    TIMESTAMPTZ,
    status      VARCHAR(20)      DEFAULT 'ACTIVE'
                CHECK (status IN ('ACTIVE', 'COMPLETED', 'ABANDONED')),
    distance_km DOUBLE PRECISION,
    avg_speed   DOUBLE PRECISION,
    max_speed   SMALLINT
);

-- Circular geofences — containment via Haversine distance
CREATE TABLE geofences (
    geofence_id   UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
    name          VARCHAR(100) NOT NULL,
    center_lat    DOUBLE PRECISION NOT NULL,
    center_lng    DOUBLE PRECISION NOT NULL,
    radius_meters INTEGER NOT NULL
);

-- Anomalies with historical flag for offline dump separation
CREATE TABLE anomalies (
    anomaly_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_id    VARCHAR(20)  NOT NULL,
    anomaly_type  VARCHAR(30)  NOT NULL,
    severity      VARCHAR(10)  NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH')),
    value         DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    detected_at   TIMESTAMPTZ  NOT NULL,
    is_historical BOOLEAN      DEFAULT false
);
```

---

## Performance Characteristics

| Metric | Value | Notes |
|---|---|---|
| Ingestion throughput | 500 msg/sec | 1,000 vehicles × 1 packet/2s |
| End-to-end latency | < 500ms | Vehicle emit → dashboard update |
| TimescaleDB write | ~50 rows/insert | Batch size 50-100, one round trip |
| Redis state read | < 1ms | In-memory, no disk I/O |
| Geofence evaluation | ~0.1ms/vehicle | NumPy vectorized Haversine, 50 zones |
| Dashboard render | < 15% CPU | Leaflet Canvas, not DOM markers |
| Worker processes | 4 | Horizontal — add more for scale |

**Scaling ceiling and what breaks first:**

At 10,000 vehicles, RabbitMQ throughput (~50k msg/sec) remains headroom. TimescaleDB write throughput becomes the first bottleneck — mitigate with larger batch sizes, PgBouncer connection pooling, and read replicas for dashboard queries. Python worker CPU becomes genuinely limiting — rewrite worker core in Go for true parallelism without process overhead.

At 100,000 vehicles, replace RabbitMQ with Kafka. `vehicle_id` as partition key guarantees per-vehicle message ordering (solving the ordering constraint RabbitMQ cannot). Consumer groups allow worker horizontal scaling with automatic partition rebalancing.

---

## Quick Start

**Prerequisites:** Docker, Docker Compose

```bash
git clone https://github.com/notjivi/fleetos
cd fleetos
cp .env.example .env
docker-compose up
```

Services start in order: TimescaleDB → Redis → RabbitMQ → API (×3) → Workers (×4) → Reaper → Simulator → Dashboard

Open `http://localhost:3000`

**To verify everything is running:**

```bash
# Check all 9 services healthy
docker-compose ps

# Watch messages flowing through RabbitMQ
open http://localhost:15672  # RabbitMQ management UI (guest/guest)

# Query live telemetry
curl http://localhost:8000/vehicles \
  -H "Authorization: Bearer {your_jwt_token}"

# Check Reaper ran
docker-compose logs reaper | tail -20
```

---

## Project Structure

```
fleetos/
├── simulator/
│   └── main.py              # 1,000 asyncio vehicle coroutines
├── gateway/
│   ├── main.py              # FastAPI WebSocket + REST endpoints
│   ├── models.py            # Pydantic schemas
│   └── middleware.py        # JWT auth + Redis rate limiting
├── worker/
│   ├── main.py              # RabbitMQ consumer, batch processor
│   ├── anomaly.py           # Rolling window detector, LRU-bounded
│   ├── geofence.py          # NumPy vectorized Haversine engine
│   └── trip.py              # State machine with SETNX locking
├── reaper/
│   └── main.py              # Ghost vehicle detection + trip closure
├── db/
│   └── schema.sql           # Full schema with indexes
├── dashboard/
│   ├── src/
│   │   ├── Map.jsx          # Leaflet Canvas renderer
│   │   ├── AnomalyFeed.jsx  # Live alert panel
│   │   └── TripHistory.jsx  # Per-vehicle trip timeline
│   └── package.json
├── docker-compose.yml       # Full stack, 9 services
├── docker-compose.scale.yml # 3 API replicas + 4 worker replicas
└── .env.example
```

---

## Architectural Decisions

Every decision below has an alternative that was considered and rejected for a specific reason.

**RabbitMQ over Kafka:** At 500 msg/sec, RabbitMQ's simpler operational model wins. Kafka's operational overhead (Zookeeper/KRaft, partition management, consumer group rebalancing) is unjustified at this scale. The architecture is Kafka-ready — the queue interface is fully abstracted. Would switch to Kafka at 50,000+ msg/sec or when per-vehicle ordering guarantees become a hard requirement.

**TimescaleDB over InfluxDB:** TimescaleDB is PostgreSQL — full SQL, window functions, JOINs between telemetry and trips tables, standard tooling, familiar operational model. InfluxDB sacrifices SQL flexibility for marginal time-series write performance improvements that don't matter at this scale.

**Redis for live state over querying TimescaleDB:** The dashboard reads current vehicle positions on every WebSocket tick. A TimescaleDB query for 1,000 vehicles' latest positions involves 1,000 `SELECT ... ORDER BY time DESC LIMIT 1` queries or a complex lateral join. Redis HGETALL on 1,000 keys is 1,000 O(1) in-memory lookups — sub-millisecond total. The 30-second TTL means stale data is bounded and expired vehicles show as offline.

**Leaflet Canvas over DOM markers:** Covered in detail in The Hard Problems section above. Short version: 1,000 DOM mutations per tick saturates the browser main thread. Canvas is one composite paint. GPU handles it.

**Python over Go for workers:** At 1,000 vehicles, Python with 4 worker processes handles the CPU load comfortably. NumPy vectorized Haversine eliminates the primary CPU bottleneck. The GIL is irrelevant with multiprocessing. Rewriting in Go would cost 2 weeks and provide no measurable improvement at this scale. The threshold where Go becomes necessary is approximately 10,000 vehicles.

---

## Environment Variables

```env
# Database
POSTGRES_HOST=timescaledb
POSTGRES_PORT=5432
POSTGRES_DB=fleetos
POSTGRES_USER=fleetos
POSTGRES_PASSWORD=changeme

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# RabbitMQ
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# Auth
JWT_SECRET=your-secret-key-here
JWT_EXPIRY_MINUTES=60

# Rate limiting
RATE_LIMIT_READ=100
RATE_LIMIT_WRITE=20
RATE_LIMIT_AUTH=5

# Worker config
WORKER_BATCH_SIZE=50
WORKER_PREFETCH=100
VEHICLE_STATE_LRU_SIZE=2000
REAPER_INTERVAL_SECONDS=300
VEHICLE_ABANDONMENT_THRESHOLD_MINUTES=10
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| API Framework | FastAPI | 0.111 |
| Data Validation | Pydantic v2 | 2.7 |
| Message Broker | RabbitMQ | 3.13 |
| AMQP Client | pika | 1.3 |
| Primary Database | TimescaleDB | 2.15 (PostgreSQL 16) |
| DB Driver | asyncpg | 0.29 |
| Cache / State | Redis | 7.2 |
| Redis Client | redis-py | 5.0 |
| In-Process LRU | cachetools | 5.3 |
| Numeric ops | NumPy | 1.26 |
| Auth | PyJWT | 2.8 |
| Scheduling | APScheduler | 3.10 |
| Frontend | React | 18 |
| Map | Leaflet.js | 1.9 (Canvas renderer) |
| Containerization | Docker Compose | v2 |

---

*FleetOS — I wanted to understand how real-time data pipelines work at scale. The only way to actually understand something is to build it and watch it break.*
