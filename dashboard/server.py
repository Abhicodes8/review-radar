"""Review Radar Swarm — orchestrator + worker agents + SSE dashboard.

One async worker agent per retail site. Pipeline per agent:
  search the site -> locate a product page -> extract reviews -> match friends
  -> (optional) score interest similarity with Claude.

Fetching uses a real headless browser (Playwright/Chromium) when installed,
falling back to plain HTTP. Review extraction uses structured JSON-LD data
when present, falling back to Claude reading the page text (needs
ANTHROPIC_API_KEY in dashboard/.env). Sites that still block are reported
honestly as "blocked" — we don't evade bot detection.
"""

import asyncio
import json
import random
import re
import time
import urllib.parse
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

import os

load_dotenv(Path(__file__).parent / ".env")

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
claude = None
if ANTHROPIC_KEY:
    import anthropic
    claude = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)

CLAUDE_MODEL = "claude-opus-5"

app = FastAPI(title="Review Radar Swarm")
STATIC = Path(__file__).parent / "static"
HEADERS = {"User-Agent": "ReviewRadar/0.2 (personal review-research tool)"}

SITES = [
    {"id": "amazon",    "name": "Amazon",     "search": "https://www.amazon.com/s?k={q}",                    "link": r"/dp/[A-Z0-9]{10}"},
    {"id": "walmart",   "name": "Walmart",    "search": "https://www.walmart.com/search?q={q}",              "link": r"/ip/[^\"'\s]+"},
    {"id": "bestbuy",   "name": "Best Buy",   "search": "https://www.bestbuy.com/site/searchpage.jsp?st={q}", "link": r"/site/[^\"'\s]+\.p\?skuId=\d+"},
    {"id": "target",    "name": "Target",     "search": "https://www.target.com/s?searchTerm={q}",           "link": r"/p/[^\"'\s]+/-/A-\d+"},
    {"id": "ebay",      "name": "eBay",       "search": "https://www.ebay.com/sch/i.html?_nkw={q}",          "link": r"/itm/\d+"},
    {"id": "etsy",      "name": "Etsy",       "search": "https://www.etsy.com/search?q={q}",                 "link": r"/listing/\d+[^\"'\s]*"},
    {"id": "newegg",    "name": "Newegg",     "search": "https://www.newegg.com/p/pl?d={q}",                 "link": r"/p/[A-Z0-9-]+\?"},
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
agents: dict[str, dict] = {}
run_info: dict = {}

# Headless browser contexts are memory-hungry; keep a small concurrency cap.
browser_sem = asyncio.Semaphore(3)
_pw = None
_browser = None


async def get_browser():
    global _pw, _browser
    if _browser is None:
        _pw = await async_playwright().start()
        _browser = await _pw.chromium.launch(headless=True)
    return _browser


def emit_agent(agent: dict):
    bus.publish({"type": "agent", "agent": agent})


def log(agent: dict, msg: str):
    agent["log"].append({"t": round(time.time() - agent["t0"], 1), "msg": msg})


def set_step(agent: dict, step: str, status: str):
    agent["steps"][step] = status
    agent["step"] = step
    emit_agent(agent)


# ── Fetching ─────────────────────────────────────────────────────────────────

BLOCK_MARKERS = ["captcha", "robot or human", "access denied", "are you a human",
                 "unusual traffic", "verify you are"]


def looks_blocked(text: str) -> bool:
    head = text[:8000].lower()
    return any(m in head for m in BLOCK_MARKERS)


async def fetch_page(agent: dict, url: str) -> dict:
    """Fetch a page, preferring a real browser. Returns
    {html, text, url, status, blocked} or raises on hard failure."""
    if HAS_PLAYWRIGHT:
        async with browser_sem:
            browser = await get_browser()
            ctx = await browser.new_context(viewport={"width": 1280, "height": 900})
            try:
                page = await ctx.new_page()
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2500)  # let reviews hydrate
                html = await page.content()
                try:
                    text = await page.inner_text("body")
                except Exception:
                    text = ""
                status = resp.status if resp else 0
                final_url = page.url
            finally:
                await ctx.close()
        log(agent, f"chromium GET {url} → {status}, {len(html) // 1000}kB")
        return {"html": html, "text": text, "url": final_url, "status": status,
                "blocked": status in (403, 429, 503) or looks_blocked(html)}

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=20) as client:
        resp = await client.get(url)
    log(agent, f"GET {url} → {resp.status_code}, {len(resp.text) // 1000}kB")
    return {"html": resp.text, "text": resp.text, "url": str(resp.url),
            "status": resp.status_code,
            "blocked": resp.status_code in (403, 429, 503) or looks_blocked(resp.text)}


# ── Friend matching ──────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower()).strip()


def is_friend(name: str, friends: list[str]) -> bool:
    n = normalize(name)
    for f in friends:
        fn = normalize(f)
        if n == fn:
            return True
        if fn and (n.startswith(fn + " ") or fn.startswith(n + " ")):
            return True
    return False


# ── Review extraction: JSON-LD, then Claude ──────────────────────────────────

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


def extract_jsonld_reviews(html: str) -> list[dict]:
    out: list[dict] = []
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S):
        try:
            _collect_reviews(json.loads(m.group(1)), out)
        except (json.JSONDecodeError, RecursionError):
            continue
    return out


REVIEWS_SCHEMA = {
    "type": "object",
    "properties": {
        "reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "author": {"type": "string"},
                    "rating": {"type": ["integer", "null"]},
                    "text": {"type": "string"},
                },
                "required": ["author", "rating", "text"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["reviews"],
    "additionalProperties": False,
}


async def claude_extract_reviews(agent: dict, page_text: str) -> list[dict]:
    log(agent, "asking Claude to extract reviews from page text")
    resp = await claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        output_config={"format": {"type": "json_schema", "schema": REVIEWS_SCHEMA}},
        messages=[{
            "role": "user",
            "content": (
                "Below is the visible text of a retail product page. Extract every "
                "customer review you can find. For each, give the reviewer's display "
                "name, star rating (1-5, or null if not shown), and the review text "
                "(trim to ~300 chars). If there are no reviews, return an empty list.\n\n"
                + page_text[:30000]
            ),
        }],
    )
    if resp.stop_reason == "refusal":
        log(agent, "Claude declined the extraction request")
        return []
    text = next((b.text for b in resp.content if b.type == "text"), "{}")
    return json.loads(text).get("reviews", [])[:25]


SIMILARITY_SCHEMA = {
    "type": "object",
    "properties": {
        "scores": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "index": {"type": "integer"},
                    "similarity": {"type": "integer"},
                    "reason": {"type": "string"},
                },
                "required": ["index", "similarity", "reason"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["scores"],
    "additionalProperties": False,
}


async def claude_score_similarity(agent: dict, reviews: list[dict], interests: str):
    """Score how much each reviewer seems to share the user's interests (0-100)."""
    listing = "\n".join(
        f"{i}. {r['author']}: {r['text'][:200]}" for i, r in enumerate(reviews)
    )
    resp = await claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        output_config={"format": {"type": "json_schema", "schema": SIMILARITY_SCHEMA}},
        messages=[{
            "role": "user",
            "content": (
                "My interests and priorities as a shopper:\n"
                f"{interests}\n\n"
                "Below are product reviews, one per line, prefixed with an index. "
                "For each review, judge from its content how much this reviewer seems "
                "to share my interests/priorities (similarity 0-100) and give a short "
                "reason (max 12 words). Score every index.\n\n" + listing
            ),
        }],
    )
    if resp.stop_reason == "refusal":
        log(agent, "Claude declined the similarity request")
        return
    text = next((b.text for b in resp.content if b.type == "text"), "{}")
    for s in json.loads(text).get("scores", []):
        i = s.get("index")
        if isinstance(i, int) and 0 <= i < len(reviews):
            reviews[i]["similarity"] = max(0, min(100, s["similarity"]))
            reviews[i]["sim_reason"] = s["reason"]
    n = sum(1 for r in reviews if r.get("similarity", 0) >= 70)
    log(agent, f"Claude scored similarity: {n} kindred reviewer(s)")


# ── Worker agent ─────────────────────────────────────────────────────────────

async def run_agent(site: dict, query: str, friends: list[str], interests: str, demo: bool):
    agent = agents[site["id"]]
    agent["status"] = "running"
    agent["t0"] = time.time()
    log(agent, f"deployed → {site['name']}" + (" (chromium)" if HAS_PLAYWRIGHT and not demo else ""))
    emit_agent(agent)

    try:
        if demo:
            await demo_agent(agent, site, friends, interests)
        else:
            await live_agent(agent, site, query, friends, interests)
        agent["status"] = "failed" if agent.get("error") else "completed"
    except Exception as e:  # keep the swarm alive if one worker dies
        agent["error"] = str(e)[:200]
        agent["status"] = "failed"
        log(agent, f"crashed: {e}")
    agent["step"] = None
    emit_agent(agent)
    bus.publish({"type": "stats", "stats": compute_stats()})


async def live_agent(agent: dict, site: dict, query: str, friends: list[str], interests: str):
    q = urllib.parse.quote_plus(query)

    # 1. search
    set_step(agent, "search", "running")
    try:
        search = await fetch_page(agent, site["search"].format(q=q))
    except Exception as e:
        agent["error"] = f"search fetch failed: {e.__class__.__name__}"
        set_step(agent, "search", "failed")
        return
    if search["blocked"]:
        agent["error"] = f"blocked by {site['name']} (HTTP {search['status']})"
        agent["blocked"] = True
        set_step(agent, "search", "failed")
        log(agent, "site refuses automated clients — not bypassing")
        return
    set_step(agent, "search", "completed")

    # 2. locate a product page
    set_step(agent, "locate", "running")
    m = re.search(site["link"], search["html"])
    if not m:
        agent["error"] = "no product link found in search results"
        set_step(agent, "locate", "failed")
        return
    product_url = urllib.parse.urljoin(search["url"], m.group(0))
    agent["product_url"] = product_url
    try:
        prod = await fetch_page(agent, product_url)
    except Exception as e:
        agent["error"] = f"product fetch failed: {e.__class__.__name__}"
        set_step(agent, "locate", "failed")
        return
    if prod["blocked"] or prod["status"] >= 400:
        agent["error"] = f"product page HTTP {prod['status']}"
        agent["blocked"] = prod["blocked"]
        set_step(agent, "locate", "failed")
        return
    set_step(agent, "locate", "completed")

    # 3. parse reviews: structured data first, then Claude on the page text
    set_step(agent, "parse", "running")
    reviews = extract_jsonld_reviews(prod["html"])
    if reviews:
        log(agent, f"{len(reviews)} reviews in structured data")
    elif claude:
        reviews = await claude_extract_reviews(agent, prod["text"])
        log(agent, f"Claude extracted {len(reviews)} reviews")
    agent["reviews"] = reviews
    set_step(agent, "parse", "completed" if reviews else "failed")
    if not reviews:
        agent["error"] = ("no structured review data" if not claude
                          else "no reviews found on page")
        return

    # 4. match friends + interest similarity
    set_step(agent, "match", "running")
    for r in agent["reviews"]:
        r["friend"] = is_friend(r["author"], friends)
    agent["matches"] = sum(1 for r in agent["reviews"] if r["friend"])
    log(agent, f"{agent['matches']} friend match(es)")
    if claude and interests.strip():
        await claude_score_similarity(agent, agent["reviews"], interests)
    set_step(agent, "match", "completed")


async def demo_agent(agent: dict, site: dict, friends: list[str], interests: str):
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
                if interests.strip() and random.random() < 0.3:
                    r["similarity"] = rng.randint(60, 97)
                    r["sim_reason"] = "shares your priorities (simulated)"
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
        "similar": sum(1 for a in vals for r in a["reviews"] if r.get("similarity", 0) >= 70),
    }


# ── API ──────────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    product: str
    friends: list[str] = []
    interests: str = ""
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
        asyncio.create_task(run_agent(site, req.product, req.friends, req.interests, req.demo))
    return {"ok": True, "agents": len(agents)}


@app.get("/api/config")
async def config():
    return {"claude": claude is not None, "playwright": HAS_PLAYWRIGHT,
            "model": CLAUDE_MODEL if claude else None}


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
