# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A multi-source used car deal finder. It scrapes listings from Facebook Marketplace, Craigslist, Cars.com, and Autotrader, enriches them with VIN decoding, NHTSA safety/recall data, and EPA fuel economy data, scores deals, and presents results in a Flask web UI.

## Running

```bash
# Activate venv (Python 3.14)
source venv/bin/activate

# Full pipeline: scrape → analyze → launch web UI
python main.py

# Skip scraping, just re-analyze existing data and launch UI
python main.py --ui-only
```

The web UI launches at `http://127.0.0.1:5000` and auto-opens in a browser.

## Configuration

`Config.json` — defines desired cars, price range, location, and which sources to enable. Required keys: `DesiredCar`, `MinPrice`, `MaxPrice`, `PriceThreshold`. The `Sources` block controls which scrapers run and their source-specific settings (CityID, region, zip, etc.). Optional `Proxy` key configures proxy rotation: `{"url": "socks5://host:port"}` for a single proxy or `{"urls": ["socks5://a:1080", "http://b:8080"]}` for random rotation (supports HTTP, SOCKS4, SOCKS5). Optional `Notifications` key configures Discord alerts: `{"discord_webhook_url": "https://discord.com/api/webhooks/...", "app_url": "https://cars.single10.app"}`.

## Architecture

**Pipeline flow:** `main.py` → scrapers → database → analysis → web UI

### Scraping Layer
- `scrapers/base.py` — `BaseScraper` ABC with shared Selenium helpers (scrolling, anti-detection delays, stealth JS injection, screenshot-on-error, yield counting via `counted_insert()`)
- `scrapers/facebook.py`, `craigslist.py`, `carscom.py`, `autotrader.py` — each implements `scrape()`, iterates over `DesiredCar` list, calls `counted_insert()` for each listing found. Cars.com and Autotrader scrapers also extract vehicle history data (owner count, accident history, deal ratings, Carfax report URLs) from listing cards
- `scrapers/__init__.py` — `ALL_SCRAPERS` registry dict mapping source name → scraper class
- `driver.py` — Firefox WebDriver factory with anti-detection settings; supports HTTP/SOCKS proxy configuration with random rotation. Two profile modes: `options.profile` (copy-per-run, dedicated `6kmbn0d4.fbscraper`) or `persistent_profile=<dir>` (in-place, survives across runs — used for Facebook so the device identity + session cookies persist). The persistent path sets `browser.startup.page=3` so FB's session-scoped `c_user`/`xs` cookies survive a clean shutdown. Does NOT override the user agent (a UA-vs-engine mismatch is what bot managers cross-check)
- `scraper_worker.py` — background threading wrapper that exposes status tracking for the web UI to poll; records per-source run metrics to `scrape_runs` table and logs yield health warnings. A source that finishes with 0 listings but a non-trivial historical average is recorded as `failed` (with a screenshot), not `completed` — so silent selector rot / bot walls surface in `/api/health` instead of looking healthy

### FlareSolverr (Cloudflare/Akamai bypass)
- `flaresolverr.py` — client for a self-hosted FlareSolverr sidecar (a real Chromium that solves bot-manager challenges). Gated on `FLARESOLVERR_URL`; unset → callers no-op/fall back. Sessions recycle every ~15 requests (FlareSolverr leaks ~20-25 MB/request and its `sessions.destroy` doesn't free it — only a container restart truly flushes, so long batch jobs should chunk + restart the container)
- **Autotrader** search is Akamai-blocked for Selenium → `scrape()` fetches pages via FlareSolverr when configured (`NEEDS_DRIVER` flips false), shared parsing via `_process_page()`
- **Cars.com** detail pages (the only place seller's notes / title status live) are Cloudflare-blocked → `enrich_listings()` routes through FlareSolverr, extracts `<section id="sellers-notes">`, scoped title detection
- **KBB** trim pages (mileage/condition-adjusted values) are WAF-blocked → `_fetch_kbb` uses FlareSolverr when available; base-price fallback applies an explicit mileage adjustment (~5%/10k mi deviation, capped ±25%)
- **KSL** needs no bypass — detail JSON is plain HTTP; `enrich_listings` drains ALL un-enriched listings per run (titleType regex)

### Facebook authentication
Facebook is the only source needing a login. The flow (`scrapers/facebook.py::_ensure_logged_in`) is layered, in priority order:
1. **Native persistent-profile session** — with `FB_PROFILE_DIR` set, the device's own `datr`+`c_user`+`xs` cookies live on disk and survive between runs, so most runs are already logged in with zero work. This is the durable path.
2. **Account-picker "Continue"** — device token still valid, resumes without a password.
3. **Backup-cookie restore** — `_load_cookies()` re-injects `fb_cookies.pkl` (saved with a forced future expiry by `_save_cookies()`) into the profile. This is the server's first-run bootstrap path.
4. **Credential auto-login** — `FB_EMAIL`/`FB_PASSWORD` (+ `FB_TOTP_SECRET` for TOTP 2FA). Heals a lapsed session headlessly, BUT headless `/login` triggers reCAPTCHA Enterprise (unsolvable) — so this is a last resort, not the primary path.

`_is_logged_in()` keys off the **`c_user` cookie** (the only reliable positive signal); HTML denylists false-positive on captcha/picker/2FA interstitials.

**Bootstrap (one-time, per machine/server):** run `bootstrap_fb_profile.py` — opens a visible Firefox on the persistent profile, waits for an interactive human login (solving any captcha/2FA once), then saves the session to both the profile and `fb_cookies.pkl`. For the headless server, ship the resulting `fb_cookies.pkl` to `/data/` so its first run restores the session into `/data/fb_profile`; thereafter it rides the persistent profile. Env vars: `FB_PROFILE_DIR`, `FB_EMAIL`, `FB_PASSWORD`, `FB_TOTP_SECRET` (all passed through `docker-compose.yml`)

### Data Layer
- `database.py` — `Database` class wrapping SQLite (`marketplace_listings.db`). Handles schema creation, migrations, and all CRUD. Key tables: `listings`, `average_prices`, `price_history`, `vin_cache`, `vehicle_ratings`, `vehicle_recalls`, `scrape_runs`
- `parsing.py` — shared helpers: `parse_price`, `parse_mileage`, `extract_year`, `parse_owner_count`, `parse_service_history`
- Listings are keyed by `(href, source)` with upsert-on-conflict logic

### Enrichment Layer (all use free public APIs, no keys needed)
- `vin.py` — VIN regex extraction from listing text + NHTSA vPIC batch decoding
- `vin_validate.py` — cross-validates VIN decode data against listing claims (year, make, drivetrain), scores mismatches as major/minor
- `nhtsa.py` — safety ratings, complaint counts, recall data; cached in `vehicle_ratings`/`vehicle_recalls` tables for 30 days
- `epa.py` — fuel economy (MPG) from fueleconomy.gov; cached alongside NHTSA data
- `trim_tiers.py` — maps trim strings to tiers 1-4 (Base → Premium) for scoring
- `drivetrain.py` — detects AWD/4WD/FWD/RWD from listing text with model-specific defaults

### Analysis & Scoring
- `analysis.py` — `clean_listings()` removes mismatches, `calculate_averages()` computes per-year per-title-group averages split by mileage bucket, `find_deals()` scores each listing
- Deal scores (0-100) are composed of 7 factors totaling 100 points:
  - **Price vs Average (30pts):** Square-root curve; moderate discounts score well
  - **Title & Condition (25pts):** Title base (clean=15, unknown=3, bad=0) + accident history (+5 to -3) + owner count (1-owner=+2, unknown=+0.5) + service history (records=+1.5) + deal rating (+0.5 to +1.5)
  - **Mileage (15pts):** Age-relative (12k mi/yr baseline) + make-specific lifespan bonus/penalty
  - **Reliability (10pts):** NHTSA stars + complaint thresholds
  - **Drivetrain (10pts):** AWD/4WD bonus (explicit=10, inferred=6)
  - **Trim Value (5pts):** Higher trim at a discount
  - **Freshness (5pts):** Days since first scraped
- Hard score caps: rebuilt≤45, salvage≤30, lemon≤25
- VIN cross-validation penalty (up to -15) applied before caps; a VIN-decoded drivetrain *overrides* the text/default guess (source `vin` counts as confirmed)
- Grades are market-relative, anchored to the empirical distribution (median listing ≈ 43): A≥75 (~top 1%), B≥62, C≥48 (above median), D≥35, F<35
- Owner count and service history parsed from listing descriptions via `parsing.py`; `detect_title_type` matches title-bearing phrases plus the reversed "the title is branded" template (AutoSavvy)
- `compute_buyer_guidance()` — per-deal negotiation playbook (suggested offer range, leverage, questions, red flags) derived from days-on-market, price cuts, market position, title/accident signals; attached in `web_ui._enrich_deals_for_render` and rendered in the deal card's expanded view
- Market timing: `calculate_averages` records daily `price_trend_snapshots`; `/api/trends` + the Analytics "Market Timing" card chart per-year asking-price lines and weekly seller price-cut counts (market softness), with a buy/wait verdict once ~a week of history accrues

### Scraper Monitoring
- `scrape_runs` table tracks every scrape run per source: start/end time, status, listings_found, errors, screenshot_path, duration
- After each source completes, yield is compared to historical average — warnings logged at <50%, critical at <20%
- On scraper crash, a browser screenshot is saved to `screenshots/` for debugging
- `/api/health` endpoint returns per-source health (good/warning/critical) and recent run history
- Analytics page renders health badges and a recent runs table with yield-vs-average percentages

### Notifications
- `notifications.py` — Discord webhook notifications triggered after each scrape completes
- **Scrape summary:** embed with total deal count, Grade A/B counts, and per-source breakdown
- **Grade A deal alerts:** individual embeds for each new Grade A deal with price, score, mileage, drivetrain, location, savings vs average, thumbnail image, and listing link
- Tracks already-notified deals in `notified_deals.txt` to prevent duplicate alerts
- Configured via `Notifications.discord_webhook_url` in Config.json; no-op if unconfigured

### Sell My Car
- `analysis.py` — `compute_sell_recommendation()` uses market percentiles + adjustments for mileage, trim, drivetrain, condition to produce recommended/quick-sell/max-value prices. Falls back to external valuations when fewer than 5 marketplace comparables exist
- `valuations.py` — External valuation lookups from KBB, Edmunds, and CarGurus via headless Selenium. KBB embeds pricing in `__NEXT_DATA__` JSON (Apollo state); overview page gives base `fppPrice` per trim, trim pages give condition-adjusted values (may be WAF-blocked). Edmunds loads the appraisal-value page, extracts style IDs from `__PRELOADED_STATE__`, then calls the internal TMV API via browser `fetch()` for mileage-adjusted private party/trade-in/dealer retail values. CarGurus uses the research/price-trends page with entity ID slugs (configurable via `cargurus_entity_id` in sell car config) for per-year average dealer prices. Results cached in `valuation_cache` table with 7-day TTL
- `config.py` — `SellCars` list in Config.json, `get_all_search_queries()` merges buy + sell car names (deduplicated) so scrapers collect data for both. Zip code for valuations pulled from `Sources.carscom.zip` or `Sources.autotrader.zip`
- Sell cars use richer config structure (name, year, mileage, title_type, trim, drivetrain, condition) vs flat `DesiredCar` strings
- Scrapers see a combined deduplicated list; differentiation happens at analysis and display layers
- `/sell` route shows pricing recommendations per sell car with market positioning, price adjustments, comparable listings, and external valuation cards (KBB, Edmunds, CarGurus)
- `/api/sell/valuations` endpoint triggers on-demand external valuation refresh
- Settings page includes "Cars to Sell" panel with multi-field form

### Facebook inline enrichment
- FB title/condition/drivetrain live only on the detail page (search cards lack them). When the worker sets `scraper.db`, the FB `scrape()` enriches **inline**: for each NEW listing it visits the detail page and inserts the row only once it has solid data (keep-only-enriched — no placeholder rows with unknown title). Already-enriched listings get a cheap price refresh (no detail visit). A per-run budget (`FB_ENRICH_BUDGET`, default 120) caps detail visits to stay under FB's rate limit; the 4×/day cadence covers the rest over ~1–2 days. `_parse_card` (card→fields) and `_visit_and_extract` (detail→fields) are the shared building blocks; the standalone `enrich_listings()` (for the `/api/enrich` route) reuses `_visit_and_extract`.

### Sold listings (market-clearing comps)
- FB marks sold items with a standalone `<span>Sold</span>` on the detail page and removes them from search. `FacebookScraper._is_sold()` detects this; `check_sold_listings()` (run after the FB scrape in `scraper_worker`) re-visits active FB listings least-recently-checked-first and flags any now sold via `db.mark_sold()`. Enrichment also detects sold inline.
- Sold listings are kept permanently (`mark_stale` skips `sold=1`) because their price is the actual sale price. `calculate_averages` weights each sold comp as `SOLD_WEIGHT` (8) asking-price samples — a weighted mean that keeps true listing counts — so averages reflect what cars really sold for, not aspirational asks.
- Sold cars are excluded from the buyable Deals/Discover lists and CSV export; they get their own `/sold` tab (`sold.html`) as comps. New Grade-A *sold* listings don't trigger Discord alerts.

### Web UI
- `web_ui.py` — Flask app with routes for deals list, sold comps, sell pricing, favorites, analytics, settings, and scraper health. Served by **waitress** (production WSGI, 8 threads); `Database` serializes all access behind a per-instance RLock because one SQLite connection is shared across request threads (two threads on one cursor segfaulted the process before)
- `templates/` — Jinja2 templates (`base.html`, `deals.html`, `sell.html`, `favorites.html`, `analytics.html`, `settings.html`, `_deal_card.html` partial)
- State files: `favorite_listings.txt`, `deleted_listings.txt` (line-delimited href sets)

## Key Patterns

- **Title groups:** Listings are grouped by title status (clean, rebuilt, salvage, lemon) for fair average comparison — a rebuilt car is scored against other rebuilt cars
- **Per-car keyword searches:** Each scraper iterates the combined buy + sell car list (`get_all_search_queries()`) and runs a separate search per car model, not a single broad search
- **Anti-detection:** Randomized delays, scroll patterns, and stealth JS injection throughout scrapers to avoid bot detection
- **Upsert with price tracking:** Re-scraped listings update in place; price changes are logged to `price_history`
- **Stale marking:** Listings not seen in 7 days get soft-deleted (`deleted_at` set)
- **Accident penalty:** Accident-reported cars receive a -3 penalty (not zero) in the condition factor — reflects 20-40% real-world value impact
- **Description parsing:** Owner count and service history signals are extracted from listing descriptions and car names via regex patterns in `parsing.py`, then fed into the scoring pipeline
- **Yield tracking:** Every scraper run records metrics to `scrape_runs`; health checks compare yield to historical norms and flag breakage
- **Screenshot on error:** `BaseScraper.capture_screenshot()` saves browser state to `screenshots/` when a scraper crashes, aiding diagnosis of selector changes or bot detection
- **Proxy rotation:** `driver.py` accepts an optional `Proxy` config and applies HTTP or SOCKS proxy settings to Firefox. When multiple URLs are provided, one is chosen at random per driver instantiation
- **Vehicle history extraction (Carfax-lite):** Cars.com and Autotrader scrapers extract owner count badges, accident history, deal ratings, and Carfax report URLs that are already displayed on listing cards — no paid API needed. Data stored in `owner_count` and `carfax_url` columns on `listings`, surfaced as badges on deal cards, and fed into the scoring pipeline

## Deployment

Runs on a home server (Dell Optiplex 7080 Micro, Ubuntu 24.04, hostname `mothership2`). Deployment config lives in sibling repo `../homelab/carscraper/`.

**Pipeline:** push to `main` → GitHub Actions builds Docker image → GHCR (`ghcr.io/snooplawg/fbmarketcarscraper:latest`) → Watchtower auto-pulls on server

**Container:**
- Entrypoint: `server.py` (loads existing deals from DB, serves UI — no scraping on startup)
- Port: 5001, memory limit 1.5 GB
- Env: `DATA_DIR=/data`, `HEADLESS=1`, `DOCKER_MODE=1`, `FLASK_HOST=0.0.0.0`
- Volume: `./data:/data` (Config.json, marketplace_listings.db, fb_cookies.pkl, favorites/deleted files)

**Server cron:**
- `0 6,11,16,21 * * *` — Scrape 4x daily via `curl -s -X POST http://localhost:5001/api/scrape`
- `0 3 * * *` — Nightly backup: `rsync` data dir to NAS (`/mnt/nas/backups/services/carscraper/`)

**Access:** `https://cars.single10.app` — reverse-proxied through Caddy, protected by Authentik SSO (Google OAuth)
