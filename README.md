# Review Radar

Find the product reviews that actually matter to you — the ones written by your friends.

## Parts

- **Chrome extension** (repo root): highlights reviews from people in `friends.js` directly on Amazon product pages. Load via `chrome://extensions` → Developer mode → Load unpacked → this folder.
- **Swarm dashboard** (`dashboard/`): HIVE-style honeycomb dashboard that deploys one agent per retail site (12 sites), streams each agent's search → locate → parse → match pipeline live over SSE, and lets you click any agent to see its activity log and matched reviews.

## Run the dashboard

```bash
python3 -m venv dashboard-venv
dashboard-venv/bin/pip install fastapi uvicorn httpx
dashboard-venv/bin/uvicorn server:app --app-dir dashboard --port 8765
```

Open http://localhost:8765. Demo mode simulates the swarm; live mode fetches real sites and honestly reports which ones block non-browser clients (most big retailers do — browser automation is the planned next step).

UI design borrowed with love from [HIVE](https://github.com/bossbobster/hive) (MIT, TreeHacks 2026).
