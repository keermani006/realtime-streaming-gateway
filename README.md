# Real-Time Streaming Gateway

A high-throughput, distributed real-time backend gateway built with **FastAPI**, **WebSockets**, **Redis Pub/Sub**, and **PostgreSQL (Asyncpg + SQLAlchemy 2.0)**, featuring horizontal scaling across multiple containerized nodes and a lightweight **React + Vite** live verification dashboard.

---

## 🎯 Architecture Overview

```
                      ┌───────────────────────────────────────────────────────────┐
                      │                   Clients & Frontends                     │
                      │  (Browser Tabs / WebSocket Clients / React Dashboard)     │
                      └─────────────┬───────────────────────────────┬─────────────┘
                                    │ WS /ws/alice                  │ WS /ws/bob
                                    ▼                               ▼
                         ┌────────────────────┐          ┌────────────────────┐
                         │  FastAPI Node 1    │          │  FastAPI Node 2    │
                         │ (Port 8000)        │          │ (Port 8001)        │
                         │                    │          │                    │
                         │ ConnectionManager  │          │ ConnectionManager  │
                         │ [Local Sockets]    │          │ [Local Sockets]    │
                         └─────────┬──────────┘          └──────────┬─────────┘
                                   │                                │
                       Publish /   │   Subscribe & Fanout           │   Publish / Subscribe
                       Broadcast   │                                │
                                   ▼                                ▼
                         ┌────────────────────────────────────────────────────┐
                         │                 Redis Pub/Sub                      │
                         │          (Shared Distributed Message Bus)          │
                         └─────────────────────────┬──────────────────────────┘
                                                   │
                                                   │ Event Persistence & Audit
                                                   ▼
                         ┌────────────────────────────────────────────────────┐
                         │                  PostgreSQL DB                     │
                         │   - clients (state & connection counts)            │
                         │   - events (audit trail & JSON payload)            │
                         │   - connection_logs (connect / disconnect history) │
                         └────────────────────────────────────────────────────┘
```

---

## 💡 Key Distributed Systems Concepts

1. **Decoupled State vs. Distributed Propagation**:
   - WebSocket connection objects (`WebSocket`) cannot be serialized or stored in Redis.
   - Each FastAPI node maintains a localized in-memory `ConnectionManager` for connected sockets.
   - Event propagation across nodes is orchestrated via Redis Pub/Sub (`gateway:events` channel).
   - If **Client A** on **Node 1** publishes an event, **Node 2** receives the Redis message and forwards it to **Client B** instantly.

2. **Database Persistence with Asyncpg & SQLAlchemy 2.0**:
   - Non-blocking I/O using async engine and session management.
   - Tracks client metadata, total connections count, live connection flags, connection logs, and event history.

3. **Heartbeat & Dead Connection Pruning**:
   - Server-driven periodic background ping/pong task (`manager.run_heartbeat`).
   - Broken pipes or disconnected clients are pruned from the active pool without blocking other clients.

4. **Resilience & Exponential Backoff**:
   - The Redis subscriber automatically handles disconnections and reconnects with exponential backoff.
   - The React frontend and Python test client automatically reconnect with exponential backoff on server restart.

---

## 📂 Project Structure

```text
realtime-streaming-gateway/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py              # FastAPI app lifespan, Redis subscriber task & heartbeat worker
│   │   ├── config.py            # Pydantic Settings & environment vars (cloud-ready)
│   │   ├── database.py          # Async SQLAlchemy engine (asyncpg), session maker
│   │   ├── models.py            # Client, Event, ConnectionLog ORM models
│   │   ├── schemas.py           # Pydantic request/response schemas
│   │   ├── websocket_manager.py # Local WebSocket connection pool & broadcast
│   │   ├── redis_manager.py     # Redis client, publisher & subscriber loop
│   │   ├── services/
│   │   │   ├── __init__.py
│   │   │   └── event_service.py # DB persistence + Redis publish orchestration
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── websocket.py     # WS /ws/{client_id} endpoint
│   │       ├── events.py        # POST /events, GET /events
│   │       └── health.py        # GET /health, GET /stats
│   ├── tests/
│   │   ├── conftest.py          # Test database and async test client fixtures
│   │   ├── test_api.py          # REST API tests
│   │   └── test_websocket.py    # Connection manager unit tests
│   ├── client_demo.py           # Resilient Python test client
│   ├── Dockerfile               # Production-ready slim container (dynamic $PORT)
│   ├── requirements.txt         # Backend Python dependencies
│   └── .env.example             # Config template
├── frontend/
│   ├── src/
│   │   ├── App.jsx              # Real-time WebSocket Dashboard (WSS & custom URL support)
│   │   ├── App.css              # Clean, modern plain CSS styling
│   │   └── main.jsx             # React entry point
│   ├── Dockerfile               # Production multi-stage Nginx container
│   ├── index.html
│   ├── package.json             # React 18 + Vite dependencies
│   └── vite.config.js
├── Caddyfile                    # Production reverse proxy with automatic HTTPS / WSS Let's Encrypt
├── render.yaml                  # 1-Click Cloud Blueprint (Postgres + Redis + API + Frontend)
├── docker-compose.yml           # Local dev cluster with 3-node scaling
├── docker-compose.prod.yml      # Production stack with automatic TLS / HTTPS / WSS
└── README.md
```

---

## 🌐 Production Deployment Guide

We provide two practical, student-friendly deployment options:

### Option 1: 1-Click Free Cloud Deployment (Render)

The project includes a ready-to-deploy `render.yaml` Blueprint that automatically provisions:
- **Managed PostgreSQL Database** (Free)
- **Redis Pub/Sub Service** (Free)
- **FastAPI Docker Web Service** (with automatic HTTPS & `wss://` WebSocket support)
- **React/Vite Static Site Dashboard** (Free)

#### Steps:
1. Push this repository to your **GitHub** account.
2. Sign in to [Render.com](https://render.com).
3. Click **New +** → **Blueprint**.
4. Select your repository.
5. Render will automatically parse `render.yaml` and create all 4 services with the environment variables wired together.
6. Click **Apply**. Once built:
   - Your backend will be live at: `https://streaming-gateway-api.onrender.com` (and `wss://...` for WebSockets).
   - Your frontend dashboard will be live at: `https://streaming-gateway-dashboard.onrender.com`.

---

### Option 2: Self-Hosted Cloud Server / VPS (AWS, DigitalOcean, Hetzner, Oracle Cloud)

Use `docker-compose.prod.yml` and `Caddyfile` for automatic free SSL/TLS certificates (HTTPS + WSS) via Let's Encrypt.

#### Steps:
1. SSH into your VPS:
   ```bash
   ssh ubuntu@your-server-ip
   ```
2. Clone the repository and navigate to the directory:
   ```bash
   git clone <your-repo-url>
   cd realtime-streaming-gateway
   ```
3. Set your domain or IP in environment:
   ```bash
   export DOMAIN=gateway.yourdomain.com  # or nip.io domain like 123.45.67.89.nip.io
   ```
4. Start the production stack:
   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   ```
5. Caddy will automatically provision Let's Encrypt certificates and serve:
   - Dashboard: `https://gateway.yourdomain.com`
   - WebSocket: `wss://gateway.yourdomain.com/ws/{client_id}`
   - REST API: `https://gateway.yourdomain.com/events`

---

## 💻 Local Testing & Scaling (Docker Compose)

### 1. Start the Cluster with 3 Scaled Nodes
```bash
docker compose up -d --build --scale api=3
```
This boots:
- `gateway_postgres` on port `5433` (internal `5432`)
- `gateway_redis` on port `6379`
- `realtime-streaming-gateway-api-1` on port `8000`
- `realtime-streaming-gateway-api-2` on port `8001`
- `realtime-streaming-gateway-api-3` on port `8002`

### 2. Start the Frontend Dashboard
```bash
cd frontend
npm install
npm run dev
```
Open **`http://localhost:3000`** in your browser.

---

## 🖥️ Live Verification Flow

1. Open **Browser Window 1** (`http://localhost:3000`):
   - Set Port to `8000` (or your remote `wss://` URL in Direct URL Mode)
   - Set Client ID to `alice`
   - Click **Connect WebSocket**
2. Open **Browser Window 2** (Incognito or second window `http://localhost:3000`):
   - Set Port to `8001` (or your remote `wss://` URL)
   - Set Client ID to `bob`
   - Click **Connect WebSocket**
3. From **Alice's window**, send an event (e.g. `order.placed` with payload `{"item": "Laptop", "price": 999}`).
4. **Observe**: Both Alice and Bob immediately receive the distributed event over Redis Pub/Sub, displaying the originating Node ID and timestamp in real time.

---

## 📡 REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Health check (verifies PostgreSQL and Redis status) |
| `GET` | `/stats` | Instance metrics, active local clients, total events |
| `POST` | `/events` | Publish a new event to DB + Redis Pub/Sub |
| `GET` | `/events` | Retrieve recent events (supports `?event_type=` filter) |
| `GET` | `/events/clients/{client_id}/logs` | Retrieve connection history for a client |
| `WS` | `/ws/{client_id}` | Persistent WebSocket streaming connection (supports `ws://` and `wss://`) |

---

## 🧪 Automated Testing

```bash
cd backend
PYTHONPATH=. pytest -v
```
