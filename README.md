# Wikinsight

Real-time analytics dashboard for [Wikipedia Recent Changes](https://stream.wikimedia.org/v2/stream/recentchange). The stack ingests the public EventSource stream, aggregates metrics in Spark, and serves live charts over Server-Sent Events (SSE) plus historical hourly data from MongoDB.

## Repository layout

| Path | Role |
|------|------|
| `frontend/` | React (CRA) dashboard |
| `backend/node/` | Express SSE API; consumes Kafka topic `processed` |
| `backend/flask/` | REST API for Mongo-backed hourly bar chart |
| `backend/stream_processing/` | Wikimedia → Kafka producer; Spark Structured Streaming processor |
| `requirements.txt` | Python dependencies (single file for Flask + streaming) |
| `.env` (create locally) | Secrets and broker URL (gitignored) |

## Data flow

```text
Wikimedia SSE (recentchange)
       → wikiToKafka.py → Kafka topic "wikipedia"
       → stream_processor.py → Kafka topic "processed" + MongoDB (wikiStats.daywise_changes)
       → Node (SSE) ────────────────────────────────┐
       → Flask (REST /hourlyChangesBarChart) ────────┼→ React app
```

- **Live widgets** (donut, line charts, leaderboard, etc.) use the browser `EventSource` API against the **Node** backend, which streams JSON derived from **`processed`**.
- **Hourly bar chart** uses **axios** against **Flask**, reading pre-aggregated day documents in MongoDB.

## Prerequisites

- **Node.js** (LTS recommended) and **npm**
- **Python 3.10+** and a virtualenv (e.g. `python -m venv wiki`)
- **Apache Kafka** (local or remote) with broker address known to all components
- **Java 11+** and **`JAVA_HOME`** set (for PySpark `stream_processor.py`)
- **MongoDB Atlas** (or compatible) cluster with database `wikiStats` and collection `daywise_changes`

## Environment variables

Create **`wikistats/.env`** at the repo root (see `.gitignore`). Typical keys:

| Variable | Purpose |
|----------|---------|
| `MONGO_URI` | Full MongoDB connection string (used by Flask and Spark) |
| `KAFKA_BROKER` | `host:9092` — same broker for producer, Spark, and Node |
| `KAFKA_TOPIC_WIKIPEDIA` | Optional; default `wikipedia` |
| `KAFKA_TOPIC_WRITE` / `KAFKA_TOPIC_READ` | Optional overrides for `stream_processor.py` |
| `WIKIMEDIA_SSE_URL` | Optional; default `https://stream.wikimedia.org/v2/stream/recentchange` |

**Flask** and **Node** load this repo’s `.env` (Node also loads `backend/node/.env` if present).

**Frontend** (optional overrides for local dev):

```text
REACT_APP_NODE_API_URL=http://localhost:8080/
REACT_APP_FLASK_API_URL=http://127.0.0.1:5000/
```

Use a trailing slash; defaults in `frontend/src/vibe/helpers/*Connector.js` point at legacy Heroku URLs if unset.

## Kafka topics

Create topics (names and partitions can match your cluster policy):

- **`wikipedia`** — raw JSON events from Wikimedia (producer)
- **`processed`** — aggregated JSON batches for the Node SSE layer (Spark producer, Node consumer)

## Setup

### Python

```bash
cd wikistats
python -m venv wiki
# Windows: wiki\Scripts\activate
# Unix:   source wiki/bin/activate
pip install -r requirements.txt
```

### Node (API + frontend)

```bash
cd backend/node && npm install
cd ../../frontend && npm install
```

The frontend uses **`frontend/.npmrc`** (`legacy-peer-deps=true`) for Chart.js 2 / `react-chartjs-2` compatibility with React 18.

## Running locally (typical order)

1. **Start Kafka** (and ZooKeeper / KRaft as required by your install).

2. **Producer** — feeds `wikipedia`:

   ```bash
   python backend/stream_processing/wikiToKafka.py
   ```

3. **Stream processor** — reads `wikipedia`, writes `processed` and Mongo:

   ```bash
   python backend/stream_processing/stream_processor.py
   ```

   Requires Spark with the Kafka connector (see `SPARK_KAFKA_PACKAGE` in `stream_processor.py` or use `spark-submit` with `--packages`).

4. **Node SSE API** (default port **8080**):

   ```bash
   cd backend/node && npm start
   ```

5. **Flask** (dev server, default **5000**):

   ```bash
   cd backend/flask
   python -m flask --app app run
   ```

6. **Frontend** (default **3000**):

   ```bash
   cd frontend && npm start
   ```

Open the URL printed by the dev server (usually `http://localhost:3000`).

## Production notes

- **Flask:** `backend/flask/Procfile` uses Gunicorn; deploy with `requirements.txt` available and `MONGO_URI` (or split Mongo credentials) set.
- **Build frontend:** `cd frontend && npm run build` — static output in `frontend/build/`.
- Ensure **CORS** and **HTTPS/mixed-content** rules match your deployment (Flask uses `flask-cors`; Node uses `cors()`).

## Screenshots & diagrams

The original project includes architecture images under `images/` (referenced from older docs); paths may point to the upstream GitHub repo.

## License / attribution

See `frontend/package.json` for author metadata. Wikipedia data is subject to Wikimedia Foundation terms of use.
