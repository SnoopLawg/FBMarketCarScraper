# Design Doc — Scaling the Scrapers Without Paying for It

**Status:** Draft / proposal
**Author:** (with Claude)
**Date:** 2026-06-13
**Constraints that shaped every decision below:** $0 budget, one machine
(`mothership2`, Dell Optiplex 7080 Micro, Ubuntu, Docker, 1.5 GB RAM cap,
headless), **one residential home IP** (single uplink, no second ISP, no
proxy pool possible).

---

## 1. Problem

Rate limiting is the consistent failure mode across all sources. The symptoms
the user actually feels:

- Favorited listings sell before we notice (the FB sold-check backlog drains
  too slowly — a 4-week-old sold Tucson sat un-flagged for days).
- The FB mileage-backfill backlog is chronic.
- KSL is **disabled in production** because PerimeterX blocks our IP.
- Cars.com / Autotrader lean on a leaky FlareSolverr sidecar (~20–25 MB/req,
  recycled every 15 requests) that is itself a throughput bottleneck.

### Root cause (three structural facts, from the code)

1. **One IP for everything.** `Config.json` → `Proxy: <none>`. Every source
   hits its target from the single home-server IP. Per-IP limits are the
   dominant signal and we concentrate *all* load onto one address.
2. **Blind pacing.** `scrapers/base.py::human_delay` is fixed random sleeps
   (2–6 s). No token bucket, no `Retry-After`, no backoff. We pace the same
   whether the site is happy or angry, and we do it in **4 bursts/day** (cron
   `0 6,11,16,21`), which is exactly the shape that trips per-minute limits.
3. **Blocking is fatal, not recoverable.** Every scraper
   (`facebook.py`, `autotrader.py`, `check_sold_listings`, `backfill_mileage`)
   does `consecutive_blocked >= 5 → break`. One transient throttle abandons the
   rest of the run's queue. This *is* the backlog problem.

Plus: sources run **sequentially behind one shared browser**
(`scraper_worker.py`), so a slow/blocked source starves the ones after it.

### The honest constraint

With one IP and no budget we **cannot out-distribute the defenses** — there is
no free way to get a pool of clean residential IPs (Tor and free proxy lists
are proactively blocklisted by Cloudflare/PerimeterX/Akamai). So this design
does **not** try. It makes the single IP we have go 3–5× further by attacking
the problem from the other side.

---

## 2. Goals / Non-goals

**Goals**
- G1. Stop *losing* work when we get throttled — block → back off → resume,
  never abandon the queue.
- G2. Cut total requests per useful row by 3–5× (the cheapest capacity there is).
- G3. Reach the **least-defended endpoint** per source (internal JSON over
  rendered HTML), shrinking block rate and retiring FlareSolverr where possible.
- G4. Pace as a steady, adaptive trickle instead of 4 bursts.
- G5. Make rate-limit health observable (feed the existing `/api/health`).

**Non-goals**
- N1. Buying proxies, ISP IPs, or managed unblockers (explicitly out of scope).
- N2. Building a multi-IP proxy pool (we have one uplink — not possible for free).
- N3. Multi-account Facebook (ban risk, complexity; out of scope).
- N4. Horizontal compute scale-out (we have one box; not the bottleneck).

---

## 3. Strategy — three free axes

Everything below is one of:

1. **Send fewer requests** (Axis-V, volume) — a request we don't send can't be
   blocked, and it scales perfectly. *Highest leverage.*
2. **Knock on the least-defended door** (Axis-E, endpoints) — internal
   JSON/GraphQL the site's own frontend calls: more data/request, fewer
   challenges, often no browser.
3. **Look human, pace like one** (Axis-P, pacing/stealth) — adaptive throttle +
   recoverable blocking + free fingerprint hygiene, so the one IP stays under
   the radar.

The single paid lever we're giving up (IP distribution) is replaced by being
*good enough* at these three that we rarely trip a limit in the first place.

---

## 4. Detailed design

### 4.1 Shared adaptive fetch layer — `netfetch.py`  *(foundation; G1, G4)*

Today each scraper re-implements `consecutive_blocked`. Replace with one module
every source routes through. Responsibilities:

- **Per-domain token bucket.** One bucket per host (sites have different
  tolerance). Steady-state throughput governor; this is also what turns the
  4-burst cron into a trickle.
- **Exponential backoff with full jitter** on a block/429:
  `sleep(random.uniform(0, min(cap, base * 2**attempt)))`.
- **Honor `Retry-After`** when present (only ~⅓ of scrapers do — it's the site
  telling us the exact wait).
- **Back-off-and-resume, not break.** Replace `>= 5 → break` with: back off,
  retry the *same* item up to N times, and only trip a **per-source circuit
  breaker** (cool the whole source for the rest of the run) after sustained
  walls. The breaker event flows to `scrape_runs` / `/api/health`.
- **AIMD adaptive concurrency/rate.** Additively raise the per-domain rate on a
  clean streak; multiplicatively cut it on a block. Self-tunes per run.

Proposed surface (transport-agnostic — browser, curl_cffi, or FlareSolverr all
plug in behind it):

```python
# netfetch.py
class Fetcher:
    def get(self, url, *, domain=None, transport="http", parse="text",
            headers=None, conditional=True): ...
    # returns FetchResult(status, body, from_cache: bool, blocked: bool)

class RateLimiter:           # per-domain token bucket + AIMD + breaker state
    def acquire(self, domain): ...      # blocks until a token is free
    def on_success(self, domain): ...   # AIMD additive increase
    def on_block(self, domain, retry_after=None): ...  # mult. decrease + backoff
```

Wire one source through it as the proof-of-concept, then migrate the rest.

### 4.2 curl_cffi transport; demote FlareSolverr  *(Axis-E/P; G3)*

PerimeterX/Cloudflare fingerprint the **TLS/JA3 handshake before any JS runs**,
so plain `requests` fails even from a clean residential IP. **curl_cffi**
impersonates a real browser's TLS for free. It becomes the default transport
for HTTP-extractable sources and is the key that:

- **re-enables KSL** (already plain JSON per `CLAUDE.md`; the only blocker is
  the IP/TLS signature), and
- lets us reach the internal JSON endpoints in §4.3.

FlareSolverr drops to **last-resort** (only when a real challenge solve is
unavoidable), shrinking its memory-leak footprint toward zero.

### 4.3 Endpoint strategy — hit the JSON  *(Axis-E; G3)*

For each source, prefer the structured endpoint the frontend itself calls
(DevTools → Network → XHR/Fetch):

| Source | Today | Target | Notes |
|---|---|---|---|
| **Autotrader** | Akamai-blocked HTML search → FlareSolverr | **Internal listings JSON API** via curl_cffi | The SPA's own backend returns full vehicle JSON; far less defended than the HTML. Likely retires FlareSolverr for this source. |
| **KSL** | disabled (PerimeterX) | **Detail JSON** via curl_cffi (+ TLS impersonation) | JSON already exists; re-enable once TLS/IP passes. |
| **Cars.com** | CF detail pages → FlareSolverr | Try **internal API** via curl_cffi first; FlareSolverr only if it still walls | Detail volume is bounded (only un-enriched listings). |
| **Facebook** | browser + login session (inline enrich) | **Keep the browser session** | GraphQL is session-bound + heavily defended; the logged-in browser is the pragmatic door. Win FB via Axis-V + §4.6 instead. |

Durable skill encoded here: *for any source, call what the site's JS calls.*

### 4.4 Volume reduction  *(Axis-V; G2 — biggest multiplier)*

- **Change-detection, not blind re-fetch.** Hash the search-card fields; only
  spend the expensive, block-prone **detail visit** when the hash changed or the
  listing is new. (Generalizes the existing cheap FB price-refresh path.)
- **Conditional requests** on HTTP sources: send `If-Modified-Since`/`ETag`,
  treat `304` as a free no-op.
- **VIN dedup across sources.** Same VIN on Cars.com *and* Autotrader → enrich
  once.
- **Incremental scrape.** Sort by newest, walk until a known ID, stop — don't
  re-pull the whole catalog every run.
- **Tiered recheck TTLs** (drives the scheduler in §4.5):
  - Tier 0 — favorites / Grade-A / freshest: every run.
  - Tier 1 — active, mid-grade: daily.
  - Tier 2 — old, unsaved, low-grade: weekly, then age out.

  This directly fixes the sold-check/mileage backlogs: the queue ordering in
  `get_active_listings_for_sold_check` stops being pure round-robin and becomes
  priority-first.

### 4.5 Scheduler — trickle, not burst  *(Axis-P; G4)*

Replace the 4×/day burst (`cron 0 6,11,16,21 → /api/scrape`) with a
**continuous low-rate drainer** governed by the §4.1 token buckets and the §4.4
priority/TTL queue. A steady drip stays under per-minute thresholds that bursts
blow through, and it keeps Tier-0 items fresh continuously instead of up to ~6 h
stale. The cron becomes a "keep the drainer alive" heartbeat rather than the
work trigger.

### 4.6 Stealth  *(Axis-P)*

- **Facebook → Camoufox.** Free, open-source Firefox with **C++-level**
  fingerprint spoofing — same engine family as today's stack (keeps the
  persistent-profile login model), but far stronger than the hand-rolled
  `navigator.webdriver`/fake-plugins JS in `driver.py`, which modern detectors
  flag *as patches*.
- **HTTP sources → curl_cffi** (§4.2) carries the fingerprint hygiene.
- Pacing (§4.1) remains the primary defense; stealth reduces the block *rate*,
  pacing makes blocks survivable.

### 4.7 Concurrency across distinct domains *(throughput, safe on one IP)*

Different domains don't share a rate limit, so running sources **concurrently**
(each under its own per-domain bucket) raises throughput without raising
per-target request rate. Note the FB browser session can't be shared (Firefox
profile lock), so FB stays single-threaded; the HTTP/JSON sources can overlap.

### 4.8 The single-IP reality (and the only escape hatch)

We have one uplink, so there is no rotation. Two manual, free fallbacks exist if
a source hard-walls the home IP:

- **Phone cellular as a Tailscale exit node** — a free *mobile* IP (hardest
  class to block) for a stuck source. One IP, uses the data plan; manual.
- **Modem-reboot IP recycling** — re-DHCP to pull a fresh IP from the ISP pool;
  disruptive (drops the household), occasional "got walled" action only.

Neither is in the automated path; they're documented escape hatches.

---

## 5. Observability  *(G5)*

Extend `scrape_runs` / `/api/health` with rate-limit signals so breakage stays
visible (the system already learned this lesson with silent FB selector rot):

- blocks & circuit-breaker trips per source per run,
- `304`/cache-hit ratio (proves Axis-V is working),
- **requests-per-listing** (the efficiency metric to drive down),
- backlog age per queue (sold-check, mileage) — Tier-0 should trend to ~0,
- effective yield vs historical (already tracked).

---

## 6. Rollout (all $0, each step shippable on its own)

1. **`netfetch.py`** — token bucket + backoff/jitter + `Retry-After` +
   back-off-and-resume + breaker. Migrate **one** source as PoC. *(Foundation.)*
2. **curl_cffi + Autotrader internal JSON** — proves the endpoint pattern,
   retires FlareSolverr for one source, no browser.
3. **Volume reduction** — change-detection hashing + VIN dedup + tiered queue
   (also fixes favorite/sold-check prioritization).
4. **Re-enable KSL** via curl_cffi (+ phone exit node only if PerimeterX bites).
5. **Trickle scheduler** — replace burst cron with the governed drainer.
6. **FB → Camoufox** — when we want FB's block rate down.
7. **Concurrency** across the HTTP/JSON sources.

**Recommended first PR: steps 1 + 2 together** — the shared adaptive fetcher,
proven by pointing Autotrader at its internal JSON via curl_cffi. Self-contained,
kills FlareSolverr maintenance for a source, and hands every later source a
reusable rate-limit core.

---

## 7. Risks & mitigations

- **Internal JSON endpoints change/disappear.** → keep the browser/FlareSolverr
  path behind the same `Fetcher` interface as a fallback; `/api/health` surfaces
  the regression fast.
- **curl_cffi still walled (PerimeterX behavioral checks).** → fall back to
  Camoufox for that source, or the phone exit node.
- **Trickle drainer + Docker restart/Watchtower** could drop in-flight state. →
  queue state lives in SQLite (durable), drainer resumes from it.
- **AIMD mistuned → starvation or over-aggression.** → conservative caps + the
  per-source circuit breaker as a backstop; log effective rate per run.

---

## 8. Open questions

- Autotrader internal API: confirm current request shape + whether it needs a
  bootstrap token/cookie (reverse-engineer via DevTools before committing §4.3).
- Cars.com: does a reachable internal listings API exist, or is the CF detail
  page genuinely the only door?
- Trickle scheduler: in-process drainer thread vs. an external `cron`-driven
  micro-batch every few minutes — which fits the Docker/Watchtower lifecycle
  better?
