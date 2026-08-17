"""Review Radar Swarm — orchestrator + worker agents + SSE dashboard.

One async worker agent per retail site. Each agent: search the site for the
product -> pick a product page -> parse JSON-LD reviews -> match friend list.
Sites that block non-browser clients are reported honestly as "blocked".
"""

import asyncio
import json
import random
import re
import time
import urllib.parse
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="Review Radar Swarm")

STATIC = Path(__file__).parent / "static"

# Honest client identity: we don't impersonate a browser or evade blocks.
HEADERS = {"User-Agent": "ReviewRadar/0.1 (personal review-research tool)"}

SITES = [
    {"id": "amazon",    "name": "Amazon",     "search": "https://www.amazon.com/s?k={q}",                    "link": r"/dp/[A-Z0-9]{10}"},
    {"id": "walmart",   "name": "Walmart",    "search": "https://www.walmart.com/search?q={q}",              "link": r"/ip/[^\"'\s]+"},
    {"id": "bestbuy",   "name": "Best Buy",   "search": "https://www.bestbuy.com/site/searchpage.jsp?st={q}", "link": r"/site/[^\"'\s]+\.p\?skuId=\d+"},
    {"id": "target",    "name": "Target",     "search": "https://www.target.com/s?searchTerm={q}",           "link": r"/p/[^\"'\s]+/-/A-\d+"},
    {"id": "ebay",      "name": "eBay",       "search": "https://www.ebay.com/sch/i.html?_nkw={q}",          "link": r"/itm/\d+"},
    {"id": "etsy",      "name": "Etsy",       "search": "https://www.etsy.com/search?q={q}",                 "link": r"/listing/\d+[^\"'\s]*"},
    {"id": "newegg",    "name": "Newegg",     "search": "https://www.newegg.com/p/pl?d={q}",                 "link": r"/p/[A-Za-z0-9-]+"},
    {"id": "homedepot", "name": "Home Depot", "search": "https://www.homedepot.com/s/{q}",                   "link": r"/p/[^\"'\s]+/\d+"},
    {"id": "wayfair",   "name": "Wayfair",    "search": "https://www.wayfair.com/keyword.php?keyword={q}",   "link": r"/pdp/[^\"'\s]+\.html"},
    {"id": "rei",       "name": "REI",        "search": "https://www.rei.com/search?q={q}",                  "link": r"/product/\d+[^\"'\s]*"},
    {"id": "lowes",     "name": "Lowe's",     "search": "https://www.lowes.com/search?searchTerm={q}",       "link": r"/pd/[^\"'\s]+"},
    {"id": "chewy",     "name": "Chewy",      "search": "https://www.chewy.com/s?query={q}",                 "link": r"/dp/\d+"},
]

STEPS = ["search", "locate", "parse", "match"]

DEMO_FIRST = ["Jordan", "Sam", "Priya", "Diego", "Mei", "Tyler", "Aisha", "Noah", "Fatima", "Leo", "Grace", "Ivan"]
DEMO_LAST = ["K.", "Alvarez", "Patel", "Chen", "B.", "Nguyen", "Okafor", "M.", "Rossi", "Kim"]
DEMO_TEXT = [
    "Been using this daily for a month and it still works like new. Worth every penny.",
    "Packaging was rough but the product itself is solid. Would buy again.",
    "Honestly exceeded my expectations for the price point.",
    "Stopped working after two weeks. Support was helpful though.",
    "Exactly as described. Fast shipping too.",
    "My friend recommended this and now I get why. Great build quality.",
    "Decent, but there are better options at this price.",
    "Five stars. The setup took two minutes and it just works.",
]


# ── Event bus ────────────────────────────────────────────────────────────────

class Bus:
    def __init__(self):
        self.clients: set[asyncio.Queue] = set()

    def publish(self, event: dict):
        for q in self.clients:
            q.put_nowait(event)


bus = Bus()
agents: dict[str, dict] = {}  # id -> agent state (current run only)
run_info: dict = {}


def emit_agent(agent: dict):
    bus.publish({"type": "agent", "agent": agent})


def log(agent: dict, msg: str):
    agent["log"].append({"t": round(time.time() - agent["t0"], 1), "msg": msg})


def set_step(agent: dict, step: str, status: str):
    agent["steps"][step] = status
    agent["step"] = step
    emit_agent(agent)


# ── Friend matching ──────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower()).strip()


def is_friend(name: str, friends: list[str]) -> bool:
    n = normalize(name)
    for f in friends:
        fn = normalize(f)
        if n == fn:
            return True
        # "Charlie" matches "Charlie K." but not "Charlotte"
        if fn and (n.startswith(fn + " ") or fn.startswith(n + " ")):
            return True
    return False


# ── Review extraction (JSON-LD) ──────────────────────────────────────────────

def _collect_reviews(node, out: list):
    if isinstance(node, dict):
        revs = node.get("review") or node.get("reviews")
        if revs:
            if isinstance(revs, dict):
                revs = [revs]
            for r in revs:
                if not isinstance(r, dict):
                    continue
                author = r.get("author")
                if isinstance(author, dict):
                    author = author.get("name")
                body = r.get("reviewBody") or r.get("description") or ""
                rating = r.get("reviewRating")
                if isinstance(rating, dict):
                    rating = rating.get("ratingValue")
                if author:
                    out.append({"author": str(author), "text": str(body)[:400], "rating": rating})
        for v in node.values():
            _collect_reviews(v, out)
    elif isinstance(node, list):
        for v in node:
            _collect_reviews(v, out)


def extract_reviews(html: str) -> list[dict]:
    out: list[dict] = []
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S):
        try:
            _collect_reviews(json.loads(m.group(1)), out)
        except (json.JSONDecodeError, RecursionError):
            continue
    return out


# ── Worker agent ─────────────────────────────────────────────────────────────

async def run_agent(site: dict, query: str, friends: list[str], demo: bool):
    agent = agents[site["id"]]
    agent["status"] = "running"
    agent["t0"] = time.time()
    log(agent, f"deployed → {site['name']}")
    emit_agent(agent)

    try:
        if demo:
            await demo_agent(agent, site, friends)
        else:
            await live_agent(agent, site, query, friends)
        agent["status"] = "failed" if agent.get("error") else "completed"
    except Exception as e:  # keep the swarm alive if one worker dies
        agent["error"] = str(e)[:200]
        agent["status"] = "failed"
        log(agent, f"crashed: {e}")
    agent["step"] = None
    emit_agent(agent)
    bus.publish({"type": "stats", "stats": compute_stats()})


async def live_agent(agent: dict, site: dict, query: str, friends: list[str]):
    q = urllib.parse.quote_plus(query)
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
        # 1. search
        set_step(agent, "search", "running")
        url = site["search"].format(q=q)
        log(agent, f"GET {url}")
        try:
            resp = await client.get(url)
        except httpx.HTTPError as e:
            agent["error"] = f"network error: {e.__class__.__name__}"
            set_step(agent, "search", "failed")
            return
        log(agent, f"HTTP {resp.status_code}, {len(resp.text) // 1000}kB")
        if resp.status_code in (403, 429, 503) or "captcha" in resp.text[:5000].lower():
            agent["error"] = f"blocked by {site['name']} (HTTP {resp.status_code})"
            agent["blocked"] = True
            set_step(agent, "search", "failed")
            log(agent, "site refuses non-browser clients — not bypassing")
            return
        set_step(agent, "search", "completed")

        # 2. locate a product page
        set_step(agent, "locate", "running")
        m = re.search(site["link"], resp.text)
        if not m:
            agent["error"] = "no product link found in search results"
            set_step(agent, "locate", "failed")
            return
        product_url = urllib.parse.urljoin(str(resp.url), m.group(0))
        agent["product_url"] = product_url
        log(agent, f"product: {product_url}")
        try:
            prod = await client.get(product_url)
        except httpx.HTTPError as e:
            agent["error"] = f"network error: {e.__class__.__name__}"
            set_step(agent, "locate", "failed")
            return
        if prod.status_code >= 400:
            agent["error"] = f"product page HTTP {prod.status_code}"
            agent["blocked"] = prod.status_code in (403, 429, 503)
            set_step(agent, "locate", "failed")
            return
        set_step(agent, "locate", "completed")

        # 3. parse reviews
        set_step(agent, "parse", "running")
        reviews = extract_reviews(prod.text)
        log(agent, f"{len(reviews)} reviews in structured data")
        agent["reviews"] = reviews
        set_step(agent, "parse", "completed" if reviews else "failed")
        if not reviews:
            agent["error"] = "page exposes no structured review data"
            return

    # 4. match friends
    set_step(agent, "match", "running")
    for r in agent["reviews"]:
        r["friend"] = is_friend(r["author"], friends)
    agent["matches"] = sum(1 for r in agent["reviews"] if r["friend"])
    log(agent, f"{agent['matches']} friend match(es)")
    set_step(agent, "match", "completed")


async def demo_agent(agent: dict, site: dict, friends: list[str]):
    """Simulated worker so the swarm UI can be exercised without live scraping."""
    rng = random.Random()
    for step in STEPS:
        set_step(agent, step, "running")
        await asyncio.sleep(rng.uniform(0.6, 2.4))
        if step == "search":
            log(agent, f"GET {site['search'].format(q='demo')} (simulated)")
        if step == "parse":
            n = rng.randint(3, 9)
            reviews = []
            for _ in range(n):
                name = f"{rng.choice(DEMO_FIRST)} {rng.choice(DEMO_LAST)}"
                if friends and rng.random() < 0.18:
                    name = rng.choice(friends)
                reviews.append({"author": name, "text": rng.choice(DEMO_TEXT),
                                "rating": rng.randint(2, 5)})
            agent["reviews"] = reviews
            log(agent, f"{n} reviews parsed (simulated)")
        if step == "match":
            for r in agent["reviews"]:
                r["friend"] = is_friend(r["author"], friends)
            agent["matches"] = sum(1 for r in agent["reviews"] if r["friend"])
            log(agent, f"{agent['matches']} friend match(es)")
        set_step(agent, step, "completed")
    if rng.random() < 0.15 and not agent["matches"]:
        agent["error"] = "rate-limited on final page (simulated)"


def compute_stats() -> dict:
    vals = agents.values()
    return {
        "total": len(agents),
        "running": sum(1 for a in vals if a["status"] == "running"),
        "completed": sum(1 for a in vals if a["status"] == "completed"),
        "failed": sum(1 for a in vals if a["status"] == "failed"),
        "reviews": sum(len(a["reviews"]) for a in vals),
        "matches": sum(a.get("matches", 0) for a in vals),
    }


# ── API ──────────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    product: str
    friends: list[str] = []
    demo: bool = False


@app.post("/api/runs")
async def start_run(req: RunRequest):
    agents.clear()
    run_info.update({"product": req.product, "demo": req.demo})
    for site in SITES:
        agents[site["id"]] = {
            "id": site["id"], "site": site["name"], "status": "queued",
            "step": None, "steps": {s: "pending" for s in STEPS},
            "reviews": [], "matches": 0, "log": [], "error": None,
            "t0": time.time(),
        }
    bus.publish({"type": "run", "run": run_info, "agents": list(agents.values()),
                 "stats": compute_stats()})
    for site in SITES:
        asyncio.create_task(run_agent(site, req.product, req.friends, req.demo))
    return {"ok": True, "agents": len(agents)}


@app.get("/api/state")
async def state():
    return {"run": run_info, "agents": list(agents.values()), "stats": compute_stats()}


@app.get("/api/events")
async def events():
    q: asyncio.Queue = asyncio.Queue()
    bus.clients.add(q)

    async def stream():
        try:
            yield f"data: {json.dumps({'type': 'hello', 'agents': list(agents.values()), 'stats': compute_stats()})}\n\n"
            while True:
                event = await q.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            bus.clients.discard(q)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")
