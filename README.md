# Review Radar

Find the product reviews that actually matter to you — the ones written by your friends, or by people who share your interests.

## Parts

- **Chrome extension** (repo root): highlights reviews from people in `friends.js` directly on Amazon product pages. Load via `chrome://extensions` → Developer mode → Load unpacked → this folder.
- **Swarm dashboard** (`dashboard/`): HIVE-style honeycomb dashboard that deploys one agent per retail site (12 sites) in parallel. Each agent runs search → locate → parse → match, streamed live over SSE; click any agent card to see its activity log and reviews. Friend matches get green badges, interest-similar ("kindred") reviewers get amber ones.

## Setup

```bash
python3 -m venv dashboard-venv
dashboard-venv/bin/pip install fastapi uvicorn httpx anthropic playwright python-dotenv
dashboard-venv/bin/playwright install chromium
```

### API key (Charlie — this bit is yours)

```bash
cp dashboard/.env.example dashboard/.env
# then edit dashboard/.env and paste your ANTHROPIC_API_KEY
```

The key powers two things in live mode: Claude reads product pages and extracts reviews when sites don't expose structured data, and it scores each reviewer's similarity to the interests you type into the dashboard. Without a key the dashboard still runs — it just falls back to structured-data extraction and friend matching only (the status banner at the top tells you what's active).

## Run

```bash
dashboard-venv/bin/uvicorn server:app --app-dir dashboard --port 8765
```

Open http://localhost:8765, enter a product, your friends, and your interests, and hit **DEPLOY SWARM**.

- **Live mode** uses headless Chromium (Playwright) to fetch real pages. Some big retailers still block automated browsers — those agents honestly report "blocked" rather than sneaking around bot detection.
- **Demo mode** simulates the swarm so you can see the whole UI working without scraping anything.

UI design borrowed with love from [HIVE](https://github.com/bossbobster/hive) (MIT, TreeHacks 2026).
